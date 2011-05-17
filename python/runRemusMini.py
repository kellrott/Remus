#!/usr/bin/env python

import sys
import json
from urllib2 import urlopen
from urllib  import quote
import remus
import imp
from cStringIO import StringIO
import callback
import remusLib

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
	def close(self):
		pass
				
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

class dummy:
	def __init__(self):
		pass
	
	def write(self, data):
		pass
	
	def close(self):
		pass

class miniCallback:
	def __init__(self):
		pass
	
	def open( self, key, name, mode ):
		print "WRITING", key, name
		return dummy()


if __name__=="__main__":
	mode = sys.argv[1]
	codePath = sys.argv[2]
	inPath = sys.argv[3]
	
	
	code = open( codePath).read()
	module = imp.new_module( "test_func" )	
	module.__dict__["__name__"] = "test_func"
	callback = callback.RemusCallback( miniCallback() )
	module.__dict__["remus"] = callback
	exec code in module.__dict__
	func = callback.getFunction( "test_func" )	
	
	outmap = { None: stdout_write(  ) }
	callback.setoutput( outmap )

	if ( mode == "map" ):
		for dkey, data in remusLib.getDataStack( inPath ):
			func( dkey, data )
				
	if ( mode == "reduce" ):
		kpURL = inPath 	
		kpData = httpGetJson( kpURL )
		for data in kpData:
			for key in data:
				func( key, data[key] )
	
	if ( mode == "pipe" ):
		inList = []
		for inFile in inPath.split(','):
			kpURL = host + inFile
			sys.stderr.write( "fetching: " + kpURL + "\n" )
			iHandle = jsonPairSplitter( urlopen( kpURL ) )
			inList.append( iHandle )
		func( inList )
		fileMap = remus.getoutput()
		for path in fileMap:
			print str(fileMap[path])

		
