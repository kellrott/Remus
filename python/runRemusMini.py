#!/usr/bin/env python

import remus
import sys
import json
from urllib2 import urlopen
from urllib  import quote
import remus
import imp
from cStringIO import StringIO

class stdout_write:
	def __init__(self):
		self.order = 0

	def emit( self, key, value ):
		print self.order, key, value
		self.order += 1
		
		
class jsonIter:
	def __init__(self, handle):
		self.handle = handle
	
	def __iter__(self):
		for line in self.handle:
			yield json.loads( line )
		self.handle.close()
	
	def read(self):
		return json.loads( self.handle.read() )

class jsonPairSplitter:
	def __init__(self, iHandle):
		self.handle = iHandle
	
	def __iter__(self):
		for line in self.handle:
			data  = json.loads( line )
			for key in data:
				yield key, data[key]
				
getCache={}

def httpGetJson( url, useCache=False ):
	if ( useCache ):
		if not getCache.has_key( url ):
			getCache[ url ] = urlopen( url ).read()			
		handle = StringIO(getCache[ url ])
	else:
		#print url
		handle = urlopen( url )
	return jsonIter( handle )


if __name__=="__main__":
	host = "http://localhost:16017/"
	remus.init(host)
	
	run = sys.argv[1]
	codePath = sys.argv[2]
	inPath = sys.argv[3]
	
	code = open( codePath).read()
	module = imp.new_module( "test_func" )	
	module.__dict__["__name__"] = "test_func"
	exec code in module.__dict__
	func = remus.getFunction( "test_func" )	
	
	outmap = { None: stdout_write(  ) }
	remus.setoutput( outmap )

	if ( run == "map" ):
		kpURL = host + inPath 
		kpData = httpGetJson( kpURL )
		for data in kpData:
			for key in data:
				func( key, data[key] )

	if ( run == "reduce" ):
		kpURL = host + inPath 	
		kpData = httpGetJson( kpURL )
		for data in kpData:
			for key in data:
				func( key, data[key] )
	
	if ( run == "pipe" ):
		inList = []
		for inFile in inPath.split(','):
			kpURL = host + inFile 
			iHandle = jsonPairSplitter( urlopen( kpURL ) )
			inList.append( iHandle )
		func( inList )
		fileMap = remus.getoutput()
		for path in fileMap:
			print str(fileMap[path])
