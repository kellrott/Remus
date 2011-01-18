#!/usr/bin/env python

import semweb
import sys
import os
from xml.dom.minidom import parseString
import imp
import json

class semWebNode(object):
	def __init__(self, id):
		self.id = id

	def loadSrc( self, code ):
		self.module = imp.new_module( self.id )	
		self.module.__dict__["__name__"] = self.id
		exec code in self.module.__dict__

class fileStreamer:
	def __init__(self, pathList):
		self.pathList = pathList
	def __iter__(self):
		for path in self.pathList:
			handle = open( path )
			for line in handle:
				yield line
			handle.close()
	
	def read(self):
		out = ""
		for path in self.pathList:
			handle = open( path )
			out += handle.read()
			handle.close()
		return out


class jsonSplitter:
	def __init__(self, iHandle):
		self.handle = iHandle
	
	def __iter__(self):
		for line in self.handle:
			yield json.loads( line )

class semWebGraph(semWebNode):
	def __init__(self, id):
		super(semWebGraph, self).__init__( id )
		self.nodes = {}
		if not os.path.exists( id ):
			os.mkdir( id )

	def addMapper(self, mapper):
		self.nodes[ mapper.id ] = mapper

	def addReducer(self, reducer):
		self.nodes[ reducer.id ] = reducer

	def addMerger(self, reducer):
		self.nodes[ reducer.id ] = reducer

	def addOutput(self, merger):
		self.nodes[ merger.id ] = merger

	def addSplitter(self, splitter):
		self.nodes[ splitter.id ] = splitter

	def cycleEngine(self):
		running = False
		for key in self.nodes:
			outPath = "%s/%s.output" % ( self.id, key )
			if ( not os.path.exists( outPath )  ):
				if ( isinstance( self.nodes[ key ].input, list ) ):
					if isinstance(self.nodes[ key ],semWebMerger):
						lRef = self.nodes[ key ].input[0]
						rRef = self.nodes[ key ].input[1]
						leftPath = "%s/%s.output" % ( self.id, lRef[1:] )
						rightPath = "%s/%s.output" % ( self.id, rRef[1:] )
						if os.path.exists( leftPath ) and os.path.exists( rightPath ):
							oHandle = open( outPath, "w" )
							self.callMerge( self.nodes[ key ], leftPath, rightPath, oHandle )
							oHandle.close()
							running = True
				elif ( self.nodes[ key ].input == "?" ):
					out = open( outPath, "w" )
					self.callNode( self.nodes[ key ], sys.stdin, out )
					out.close()
					running = True
				elif ( self.nodes[ key ].input is None or len( self.nodes[ key ].input ) == 0 ):
					out = open( outPath, "w" )
					self.callNode( self.nodes[ key ], None, out )
					out.close()
					running = True
				else:
					fileList = []
					ready = True
					for ref in self.nodes[ key ].input.split(","):
						if ( ref.startswith(":") ):
							inPath = "%s/%s.output" % ( self.id, ref[1:] )
							if ( os.path.exists( inPath ) ):
								fileList.append( inPath )
							else:
								ready = False
						else:
							if ( os.path.exists( ref ) ):
								fileList.append( ref )
							else:
								ready = False
					
					if ready:
						oHandle = open( outPath, "w" )
						iHandle = fileStreamer( fileList )
						self.callNode( self.nodes[ key ], iHandle, oHandle )
						oHandle.close()
						running = True
		return running
	
	def callMerge( self, node, leftPath, rightPath, oHandle ):
		semweb.setoutput( oHandle )
		curFunc = semweb.getFunction( node.id )
		print "Running Merger", node.id
		lHandle = open( leftPath )
		lSort = {}
		for lLine in lHandle:
			lIn = json.loads( lLine )
			k = lIn.keys()[0]
			if lSort.has_key( k ):
				lSort[ k ].append( lIn[k] )
			else:
				lSort[ k ] = [ lIn[k] ]			
		lHandle.close()
		
		rHandle = open( rightPath )
		rSort = {}
		for rLine in rHandle:
			rIn = json.loads( rLine )
			k = rIn.keys()[0]
			if rSort.has_key( k ):
				rSort[ k ].append( rIn[k] )
			else:
				rSort[ k ] = [ rIn[k] ]			
		rHandle.close()
		
		for l in lSort:
			for r in rSort:
				curFunc( l, lSort[l], r, rSort[r] ) 

	
	def callNode(self, node, inHandle, outHandle):
		semweb.setoutput( outHandle )
		curFunc = semweb.getFunction( node.id )
		if isinstance(node,semWebMapper):
			print "Running Mapper", node.id
			for line in inHandle:
				j = json.loads( line )
				for k in j:
					curFunc( k, j[k] )
		
		elif isinstance(node,semWebSplitter):
			print "Running Splitter", node.id
			curFunc( inHandle )
		
		elif isinstance(node,semWebReducer):
			print "Running Reducer", node.id
			reduce_sort = {}
			for line in inHandle:
				j = json.loads( line )
				k = j.keys()[0]
				if reduce_sort.has_key( k ):
					reduce_sort[ k ].append( j[k] )
				else:
					reduce_sort[ k ] = [ j[k] ]			
			for key in reduce_sort:
				curFunc( key, reduce_sort[key] )
		
		elif isinstance(node,semWebOutput):
			print "Running Output", node.id		
			jSplitter = jsonSplitter( inHandle )
			curFunc( jSplitter )
			
class semWebMapper(semWebNode):
	def __init__(self, id, input):
		super(semWebMapper, self).__init__(id)
		self.input = input

class semWebReducer(semWebNode):
	def __init__(self, id, input):
		super(semWebReducer, self).__init__(id)
		self.input = input		

class semWebSplitter(semWebNode):
	def __init__(self, id, input):
		super(semWebSplitter, self).__init__(id)
		self.input = input		


class semWebMerger(semWebNode):
	def __init__(self, id, left, right ):
		super(semWebMerger, self).__init__(id)
		self.input = [ left, right ]


class semWebOutput(semWebNode):
	def __init__(self, id, input):
		super(semWebOutput, self).__init__(id)
		self.input = input		

def getText( node ):
	out = ""
	for c in node:
		out += c.data
	return out

if __name__=="__main__":
	semweb.init()
	
	graph = semWebGraph("outgraph")
	
	handle = open( sys.argv[1] )
	data = handle.read()
	handle.close()
	dom = parseString(data)
	mappers = dom.getElementsByTagName('semweb_mapper')
	for mapper in mappers:
		mapperInput = mapper.getAttribute("input")
		mapperFile = mapper.getAttribute("include")
		mapperID = mapper.getAttribute("id")
		code = getText( mapper.childNodes )				
		m = semWebMapper( mapperID, mapperInput )
		m.loadSrc( code )
		graph.addMapper( m )
	
	reducers = dom.getElementsByTagName("semweb_reducer")
	for reducer in reducers:
		reducerInput = reducer.getAttribute("input")
		reducerFile = reducer.getAttribute("include")
		reducerID = reducer.getAttribute("id")
		code = getText( reducer.childNodes )				
		m = semWebReducer( reducerID, reducerInput )
		m.loadSrc( code )
		graph.addReducer( m )
		
	outputs = dom.getElementsByTagName("semweb_output")
	for output in outputs:
		outputInput = output.getAttribute("input")
		outputFile = output.getAttribute("include")
		outputID = output.getAttribute("id")
		code = getText( output.childNodes )				
		m = semWebOutput( outputID, outputInput )
		m.loadSrc( code )
		graph.addOutput( m )
	
	
	splitters = dom.getElementsByTagName("semweb_splitter")
	for splitter in splitters:
		splitterInput = splitter.getAttribute("input")
		splitterFile = splitter.getAttribute("include")
		splitterID = splitter.getAttribute("id")
		code = getText( splitter.childNodes )				
		m = semWebSplitter( splitterID, splitterInput )
		m.loadSrc( code )
		graph.addSplitter( m )
		
	mergers = dom.getElementsByTagName("semweb_merger")
	for merger in mergers:
		mergerLeft = merger.getAttribute("left")
		mergerRight = merger.getAttribute("right")
		mergerFile = merger.getAttribute("include")
		mergerID = merger.getAttribute("id")
		code = getText( merger.childNodes )				
		m = semWebMerger( mergerID, mergerLeft, mergerRight )
		m.loadSrc( code )
		graph.addMerger( m )


	while ( graph.cycleEngine() ):
		pass
	
