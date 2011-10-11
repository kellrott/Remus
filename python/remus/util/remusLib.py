
import json
import sys	
from urlparse import urlparse
import httplib
from cStringIO import StringIO
import remusDB
verbose = True
global workerID
import copy
import urllib


curServer = None
curConn = None
workerID= None

def urlopen(url,data=None,retry=1):
	#print "getting", url
	u = urlparse( url )
	global curConn
	global curServer
	if curConn is None or curServer != u.netloc:
		curConn = httplib.HTTPConnection(u.netloc)
		curServer = u.netloc
	try:
		if data is not None:
			headers = {"Cookie":  'remusWorker=%s' % (workerID) }
			curConn.request("POST", u.path, data, headers)
			return StringIO( curConn.getresponse().read() )
		else:
			headers = {"Cookie":  'remusWorker=%s' % (workerID) }
			path = u.path
			if len(u.query):
				path += "?" + u.query
			curConn.request("GET", path, None, headers)
			return StringIO( curConn.getresponse().read() )
	except httplib.BadStatusLine:
		if retry > 0:
			curConn = None
			urlopen( url, data, retry-1)

def quote( inStr ):
	return urllib.quote( inStr )

def log(v):
	"""
	Generic logging code, replace with something better later
	"""
	if ( verbose ):
		sys.stderr.write( v + "\n" )

def httpPostJson( url, data ):
	log( "posting:" + url )
	handle = urlopen( url, json.dumps(data) )
	return jsonIter( handle )


def httpGetJson( url ):
	log( "getting: " + url )
	handle = urlopen( url )
	return jsonIter( handle )


class jsonIter:
	"""
	take handle, read one line at a time,
	parse to sequential json objects, and return them
	one at a time
	"""
	def __init__(self, handle):
		self.handle = handle
	
	def __iter__(self):
		try:
			for line in self.handle:
				yield json.loads( line )
		except ValueError:
			pass
		self.handle.close()
	
	def read(self):
		a = self.handle.read()
		print a
		return json.loads( a )

class getDataStack:
	def __init__(self, stackDB, key=None, reduce=False, dataOnly=False):
		self.stackDB = stackDB
		self.selectKey = key
		self.reduce = reduce
		self.dataOnly = dataOnly

	def __iter__(self):
		if self.selectKey is not None:		
			valList = list( self.stackDB.get( self.selectKey ) )
			if self.reduce:
				if len( valList ):
					if self.dataOnly:
						yield valList
					else:
						yield self.selectKey, valList
			else:
				for value in valList:
					if self.dataOnly:
						yield value
					else:
						yield self.selectKey, value
		else:	
			collect = []
			curKey = None
			for key, value in self.stackDB.listKVPairs():
				if self.reduce:
					if curKey is not None and key != curKey:
						if self.dataOnly:
							yield collect						
						else:
							yield curKey, collect
						collect = []
					curKey = key
					collect.append( value )
				else:
					if self.dataOnly:
						yield value					
					else:
						yield key, value
			if self.reduce and curKey is not None:
				if self.dataOnly:
					yield collect
				else:
					yield curKey, collect


def setWorkerID(id):
	global workerID
	workerID = id

global defaultStackInterface
defaultStackInterface = None
global interfaceDict
interfaceDict = {}

global defaultAttachInterface
defaultAttachInterface = None
global attachDict
attachDict = {}

global workerDict
workerDict = {}

def addWorker( type, callback ):
	global workerDict
	workerDict[ type ] = callback

def setStackDB( name, stackDB ):
	global defaultStackInterface
	global interfaceDict
	defaultStackInterface = name
	interfaceDict[ name ] = stackDB

def setAttachDB( name, attachDB ):
	global defaultAttachInterface
	global attachDict
	defaultAttachInterface = name
	attachDict[ name ] = attachDB


def getWorker( host, pipeline, instance, applet ):
	global workerDict
	global workerID
	
	appletPath = instance + "/" + pipeline + "/" + applet 
	if workerDict.has_key( appletPath ):
		return workerDict[ appletPath ]

	appletDesc = None
	for data in httpGetJson( host + pipeline + "/" + instance + "/@status/" + applet ):
		for key in data:
			appletDesc = data[ key ]
	print appletDesc
	workerType = appletDesc[ '_type' ]
	if not workerDict.has_key( workerType ):
		raise Exception("Unknown code type: %s" % (workerType) )

	worker = workerDict[ workerType ]( appletDesc['_mode'] )(host, workerID, pipeline, instance, applet, appletDesc)	
	
	if worker is not None:
		workerDict[ appletPath ] = worker
	return worker


class StackWrapper:
	def __init__(self, host, workerID, pipeline, instance, applet, useHTTP=False ):
		global defaultStackInterface
		global interfaceDict	
		self.stack = None
		if not useHTTP:
			try:
				self.stack = interfaceDict[defaultStackInterface]( host, workerID, pipeline, instance, applet )
			except Exception, e:
				log( 'INIT FAIL:' + str(defaultStackInterface) + str(e) )
				pass
		
		if self.stack is None:
			self.stack = interfaceDict['http']( host, workerID, pipeline, instance, applet )
		
	def get(self, key):
		return self.stack.get( key )
		
	def put(self, key, jobID, emitID, value):
		return self.stack.put( key, jobID, emitID, value )

	def listKVPairs(self):
		return self.stack.listKVPairs()

	def close(self):
		return self.stack.close()

class AttachWrapper:
	def __init__(self, server, workerID, pipeline, instance, applet ):
		global defaultAttachInterface
		global attachDict
		self.attach = None
		try:
			self.attach = attachDict[ defaultAttachInterface ]( server, workerID, pipeline, instance, applet )
		except Exception, e:
			print e
			self.attach = attachDict[ 'http' ]( server, workerID, pipeline, instance, applet )
	
	def open(self, key, name, mode="r" ):
		return self.attach.open(key, name, mode)

import remusDB_http
import remusDB_file
import pythonWorker
