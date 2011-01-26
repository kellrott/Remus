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


class jsonPairSplitter:
	def __init__(self, iHandle):
		self.handle = iHandle
	
	def __iter__(self):
		for line in self.handle:
			data  = json.loads( line )
			for key in data:
				yield key, data[key]
				
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
							out = { None : open( outPath, "w" ) }
							if self.nodes[ key ].output is not None:
								for oName in self.nodes[ key ].output.split(","):
									out[ oName ] = open( "%s/%s.%s.output" % (self.id, key, oName), "w" )
							self.callMerge( self.nodes[ key ], leftPath, rightPath, out )
							for oKey in out:
								out[ oKey ].close()
							running = True
				elif ( self.nodes[ key ].input == "?" ):
					out = { None : open( outPath, "w" ) }
					if self.nodes[ key ].output is not None:
						for oName in self.nodes[ key ].output.split(","):
							out[ oName ] = open( "%s/%s.%s.output" % (self.id, key, oName), "w" )
					self.callNode( self.nodes[ key ], sys.stdin, out )
					for oKey in out:
						out[ oKey ].close()
					running = True
				elif ( self.nodes[ key ].input is None or len( self.nodes[ key ].input ) == 0 ):
					out = { None : open( outPath, "w" ) }
					if self.nodes[ key ].output is not None:
						for oName in self.nodes[ key ].output.split(","):
							out[ oName ] = open( "%s/%s.%s.output" % (self.id, key, oName), "w" )
					self.callNode( self.nodes[ key ], None, out )
					for oKey in out:
						out[ oKey ].close()
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
						out = { None : open( outPath, "w" ) }
						if self.nodes[ key ].output is not None:
							for oName in self.nodes[ key ].output.split(","):
								out[ oName ] = open( "%s/%s.%s.output" % (self.id, key, oName), "w" )
						iHandle = fileStreamer( fileList )
						self.callNode( self.nodes[ key ], iHandle, out )
						for oKey in out:
							out[ oKey ].close()
						running = True
		return running
	
	def callMerge( self, node, leftPath, rightPath, outmap ):
		semweb.setoutput( outmap )
		curFunc = semweb.getFunction( node.id )
		sys.stderr.write( "Running Merger %s\n" % (node.id) )
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

	
	def callNode(self, node, inHandle, outMap):
		semweb.setoutput( outMap )
		curFunc = semweb.getFunction( node.id )
		if isinstance(node,semWebMapper):
			sys.stderr.write( "Running Mapper %s\n" % (node.id) )
			for line in inHandle:
				j = json.loads( line )
				for k in j:
					curFunc( k, j[k] )
		
		elif isinstance(node,semWebSplitter):
			sys.stderr.write( "Running Splitter %s\n" % (node.id) )
			curFunc( inHandle )
		
		elif isinstance(node,semWebReducer):
			sys.stderr.write( "Running Reducer %s\n" % (node.id) )
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
			sys.stderr.write( "Running Outputer %s\n" % (node.id) )
			jSplitter = jsonPairSplitter( inHandle )
			curFunc( jSplitter )

class semWebXmlNode(semWebNode):
	def __init__(self, xmlNode):
		inputStr   = xmlNode.getAttribute("input")
		includeStr = xmlNode.getAttribute("include")
		idStr      = xmlNode.getAttribute("id")
		self.input = inputStr
		if xmlNode.hasAttribute("output"):
			self.output = xmlNode.getAttribute("output")
		else:
			self.output = None
		super(semWebXmlNode, self).__init__(idStr)		
		code = getText( xmlNode.childNodes )				
		self.loadSrc( code )

class semWebMapper(semWebXmlNode):
	def __init__(self, xmlNode):
		super(semWebMapper, self).__init__(xmlNode)

class semWebReducer(semWebXmlNode):
	def __init__(self, xmlNode):
		super(semWebReducer, self).__init__(xmlNode)

class semWebSplitter(semWebXmlNode):
	def __init__(self, xmlNode):
		super(semWebSplitter, self).__init__(xmlNode)

class semWebMerger(semWebXmlNode):
	def __init__(self, xmlNode ):
		super(semWebMerger, self).__init__(xmlNode)
		mergerLeft = xmlNode.getAttribute("left")
		mergerRight = xmlNode.getAttribute("right")
		self.input = [ mergerLeft, mergerRight ]

class semWebOutput(semWebXmlNode):
	def __init__(self, xmlNode):
		super(semWebOutput, self).__init__(xmlNode)

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
		m = semWebMapper( mapper )
		graph.addMapper( m )
	
	reducers = dom.getElementsByTagName("semweb_reducer")
	for reducer in reducers:
		m = semWebReducer( reducer )
		graph.addReducer( m )
		
	outputs = dom.getElementsByTagName("semweb_output")
	for output in outputs:
		m = semWebOutput( output )
		graph.addOutput( m )
	
	splitters = dom.getElementsByTagName("semweb_splitter")
	for splitter in splitters:
		m = semWebSplitter( splitter )
		graph.addSplitter( m )
		
	mergers = dom.getElementsByTagName("semweb_merger")
	for merger in mergers:
		m = semWebMerger( merger )
		graph.addMerger( m )


	while ( graph.cycleEngine() ):
		pass
	
