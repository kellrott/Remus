#!/usr/bin/env python

import sys
import json
from urllib2 import urlopen
from urllib  import quote
from urlparse import urlparse
import remus
import imp
from cStringIO import StringIO
import callback
import remusLib
import os

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
	def __init__(self, path, appletDesc ):
		a = urlparse( path )
		self.server = a.scheme + "://" + a.netloc + "/"
		self.url = path
		self.appletDesc = appletDesc
		self.outdir = "mini"
		if not os.path.exists( self.outdir ):
			os.makedirs( self.outdir )
			
	def open( self, key, name, mode ):
		if mode == "w":
			handle = open( os.path.join( self.outdir, "%s.%s" % ( key, name ) ), "w" )
			return handle
	
	def keylist( self, applet ):
		print "stack", self.url + "?info"
		info = json.loads( urlopen( self.url + "?info" ).read() )
		keyURL = self.server + info["_pipeline"] + "/" + applet.replace(":", "/")
		handle = urlopen( keyURL )
		for line in handle:
			yield json.loads( line )

	def get( self, applet, key ):
		print "stack", self.url + "?info"
		info = json.loads( urlopen( self.url + "?info" ).read() )
		keyURL = self.server + info["_pipeline"] + "/" + applet.replace(":", "/") + "/" + key
		handle = urlopen( keyURL )
		for line in handle:
			yield json.loads( line )[ key ]

	def getInfo( self, name ):
		return self.appletDesc.get( name, None ) 

if __name__=="__main__":
	pipePath = sys.argv[1]
	applet   = sys.argv[2]
	instPath = sys.argv[3]

	pipeline = json.loads( open( pipePath ).read() )[ applet ]

	for arg in sys.argv[4:]:
		tmp = arg.split( '=' )
		pipeline[ tmp[0] ] = tmp[1]
	

	code = open( pipeline['_code'] ).read()
	module = imp.new_module( "test_func" )	
	module.__dict__["__name__"] = "test_func"
	callback = callback.RemusCallback( miniCallback(instPath, pipeline) )
	module.__dict__["remus"] = callback
	exec code in module.__dict__
	func = callback.getFunction( "test_func" )	

	if ( pipeline['_mode'] == "map" ):
		for dkey, data in remusLib.getDataStack( inPath ):
			func( dkey, data )
				
	if ( pipeline['_mode'] == "reduce" ):
		kpURL = inPath 	
		kpData = httpGetJson( kpURL )
		for data in kpData:
			for key in data:
				func( key, data[key] )
	
	if ( pipeline['_mode'] == "pipe" ):
		inList = []
		for inFile in pipeline['_src'].split(','):
			kpURL = instPath + "/" + inFile
			sys.stderr.write( "fetching: " + kpURL + "\n" )
			iHandle = remusLib.getDataStack( kpURL )
			inList.append( iHandle )
		func( inList )
		fileMap = remus.getoutput()
		for path in fileMap:
			print str(fileMap[path])

		
