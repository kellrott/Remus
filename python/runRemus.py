#!/usr/bin/env python

import remus
import sys
import os
import json

from remusLib import *

class file_output:
	def __init__(self, path):
		self.handle = open( path, "w" )
	
	def emit(self, key, value):
		self.handle.write( json.dumps( {key : value} ) + "\n" )
	
	def close(self):
		self.handle.close()
				
class remusGraph(remusNode):
	def __init__(self, id):
		super(remusGraph, self).__init__( id )
		self.nodes = {}
		if not os.path.exists( id ):
			os.mkdir( id )

	def addNode(self, node):
		self.nodes[ node.id ] = node

	def cycleEngine(self):
		running = False
		for key in self.nodes:
			outPath = "%s/%s.output" % ( self.id, key )
			if ( not os.path.exists( outPath )  ):
				if ( isinstance( self.nodes[ key ].input, list ) ):
					if isinstance(self.nodes[ key ],remusMerger):
						lRef = self.nodes[ key ].input[0]
						rRef = self.nodes[ key ].input[1]
						leftPath = "%s/%s.output" % ( self.id, lRef[1:] )
						rightPath = "%s/%s.output" % ( self.id, rRef[1:] )
						if os.path.exists( leftPath ) and os.path.exists( rightPath ):
							out = { None : file_output( outPath ) }
							if self.nodes[ key ].output is not None:
								for oName in self.nodes[ key ].output.split(","):
									out[ oName ] = file_output( "%s/%s.%s.output" % (self.id, key, oName) )
							self.callMerge( self.nodes[ key ], leftPath, rightPath, out )
							for oKey in out:
								out[ oKey ].close()
							running = True
				elif ( self.nodes[ key ].input == "?" ):
					out = { None : file_output( outPath ) }
					if self.nodes[ key ].output is not None:
						for oName in self.nodes[ key ].output.split(","):
							out[ oName ] = file_output( "%s/%s.%s.output" % (self.id, key, oName) )
					self.callNode( self.nodes[ key ], sys.stdin, out )
					for oKey in out:
						out[ oKey ].close()
					running = True
				elif ( self.nodes[ key ].input is None or len( self.nodes[ key ].input ) == 0 ):
					out = { None : file_output( outPath ) }
					if self.nodes[ key ].output is not None:
						for oName in self.nodes[ key ].output.split(","):
							out[ oName ] = file_output( "%s/%s.%s.output" % (self.id, key, oName) )
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
						out = { None : file_output( outPath ) }
						if self.nodes[ key ].output is not None:
							for oName in self.nodes[ key ].output.split(","):
								out[ oName ] = file_output( "%s/%s.%s.output" % (self.id, key, oName) )
						iHandle = fileStreamer( fileList )
						self.callNode( self.nodes[ key ], iHandle, out )
						for oKey in out:
							out[ oKey ].close()
						running = True
		return running
	
	def callMerge( self, node, leftPath, rightPath, outmap ):
		remus.setoutput( outmap )
		curFunc = remus.getFunction( node.id )
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
		remus.setoutput( outMap )
		curFunc = remus.getFunction( node.id )
		if isinstance(node,remusMapper):
			sys.stderr.write( "Running Mapper %s\n" % (node.id) )
			for line in inHandle:
				j = json.loads( line )
				for k in j:
					curFunc( k, j[k] )
		
		elif isinstance(node,remusSplitter):
			sys.stderr.write( "Running Splitter %s\n" % (node.id) )
			curFunc( inHandle )
		
		elif isinstance(node,remusReducer):
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
		
		elif isinstance(node,remusOutput):
			sys.stderr.write( "Running Outputer %s\n" % (node.id) )
			jSplitter = jsonPairSplitter( inHandle )
			curFunc( jSplitter )


if __name__=="__main__":
	remus.init()
	
	graph = remusGraph("outgraph")
	handle = open( sys.argv[1] )	
	parseRemus( graph, handle )
	handle.close()

	while ( graph.cycleEngine() ):
		pass
	
