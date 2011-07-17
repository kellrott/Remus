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


class miniFileCallBack:

	def __init__(self, path, appletName, appletDesc ):
		self.appletName = appletName
		self.appletDesc = appletDesc
		self.outdir = "mini"
		if not os.path.exists( self.outdir ):
			os.makedirs( self.outdir )
		self.outHandles = { None : open( os.path.join( self.outdir, self.appletName + "@data" ), "w") }
		if '_output' in appletDesc:
			for port in appletDesc['_output']:
				self.outHandles[ port ] = open( os.path.join( self.outdir, self.appletName + "." + port + "@data" ), "w" )
			
			
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

	def emit(self, key, value, port):
		self.outHandles[ port ].write( "%s\t%s\n" % (key, json.dumps(value)) )

	def getInfo( self, name ):
		return self.appletDesc.get( name, None ) 
	
	def close( self ):
		for port in self.outHandles:
			self.outHandles.close()


class miniNetCallback(miniFileCallBack):
	def __init__(self, path, appletName, appletDesc ):
		miniFileCallBack.__init__( path, appletName, appletDesc )
		a = urlparse( path )
		self.server = a.scheme + "://" + a.netloc + "/"
		self.url = path
	
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


class ServerWrapperGen:
	def __init__(server, pipeline, instance):
		self.server = server
		self.pipeline = pipeline
		self.instance = instance
	
	def getWapper( self, applet ):
		return remusLib.StackWrapper( self.server, "DEBUG", self.pipeline, self.instance, applet )

class FileWrapper:
	def __init__(self, path):
		self.path = path
	
	def listKVPairs(self):
		handle = open( self.path )
		for line in handle:
			tmp = line.split("\t")
			yield tmp[0], json.loads( tmp[1] )

class FileWrapperGen:
	def __init__(self, path):
		self.path = path
	
	def getWrapper( self, applet ):
		return FileWrapper( os.path.join( self.path, applet + "@data" ) )

def main( argv ):
	from getopt import getopt
	opts, args = getopt( argv, "m:" )
	
	print args

	pipePath = args[0]
	applet   = args[1]
	input    = args[2]

	for arg in args[3:]:
		tmp = arg.split( '=' )
		appletDesc[ tmp[0] ] = tmp[1]

	appletDesc = json.loads( open( pipePath ).read() )[ applet ]
	
	if input.startswith( "http://" ) or input.startswith( "https://" ):
		h = urlparse( instPath )
		server = "%s://%s" % ( h.scheme, h.netloc )
		tmp = h.path.split("/")
		pipeline = tmp[1]
		instance = tmp[2]
		cb = callback.RemusCallback( miniNetCallback(input, applet, appletDesc) )
		wrapperFactory = FileWrapperGen( server, pipeline, instance )
	else:
		test = "hello"
		cb = callback.RemusCallback( miniFileCallBack(input, applet, appletDesc) )
		wrapperFactory = FileWrapperGen( input )
	

	code = open( appletDesc['_code'] ).read()
	module = imp.new_module( "test_func" )	
	module.__dict__["__name__"] = "test_func"
	module.__dict__["remus"] = cb
	exec code in module.__dict__
	func = cb.getFunction( "test_func" )	
		
	if (appletDesc['_mode'] == "split"):
		handle = open( input )
		data = json.loads( handle.read() )
		func( data )

	if ( appletDesc['_mode'] == "map" ):
		for dkey, data in remusLib.getDataStack( wrapperFactory.getWrapper( appletDesc["_src"] ) ):
			func( dkey, data )
				
	if ( appletDesc['_mode'] == "reduce" ):
		for dkey, data in remusLib.getDataStack( wrapperFactory.getWrapper( appletDesc["_src"] ), reduce=True ):
			func( dkey, data )
	
	if ( appletDesc['_mode'] == "pipe" ):
		inList = []
		for inFile in appletDesc['_src']:			
			dStack = wrapperFactory.getWrapper( inFile )			
			iHandle = remusLib.getDataStack( dStack )
			inList.append( iHandle )
		func( inList )

		


if __name__=="__main__":
	main( sys.argv[1:] )


