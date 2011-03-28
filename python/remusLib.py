
import json
import sys	
from urlparse import urlparse
import httplib
from cStringIO import StringIO
verbose = True
global workerID
import copy

curServer = None
curConn = None
workerID= None

def urlopen(url,data=None,retry=1):
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
			curConn.request("GET", u.path, None, headers)
			return StringIO( curConn.getresponse().read() )
	except httplib.BadStatusLine:
		if retry > 0:
			curConn = None
			urlopen( url, data, retry-1)



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

	
class httpStreamer:
	"""
	Class that reads off list of urls, opens each of the them sequentially
	then returns the results one line at a time	
	"""
	def __init__(self, pathList):
		self.pathList = pathList
	def __iter__(self):
		for path in self.pathList:
			handle = urlopen( path )
			for line in handle:
				yield line
			handle.close()
	
	def read(self):
		out = ""
		for path in self.pathList:
			handle = urlopen( path )
			out += handle.read()
			handle.close()
		return out


getCache={}

def httpGetJson( url, useCache=False ):
	log( "getting: " + url )
	if ( useCache ):
		if not getCache.has_key( url ):
			getCache[ url ] = urlopen( url ).read()			
		handle = StringIO(getCache[ url ])
	else:
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

class valueIter:
	"""
	take jsonIter, but return only the first level values 
	in each object
	"""
	def __init__(self, handle):
		self.handle = handle
	
	def __iter__(self):
		for v in self.handle:
			for k in v:
				yield v[k]
	
class jsonPairSplitter:
	"""

	"""
	def __init__(self, iHandle):
		self.handle = iHandle
	
	def __iter__(self):
		for line in self.handle:
			data  = json.loads( line )
			for key in data:
				yield key, data[key]
	def close(self):
		pass


class http_write:
	def __init__(self, url, jobID):
		self.url = url
		self.order = 0
		self.jobID = jobID
		self.cache = []
		self.cacheMax = 10000
		
	def emit( self, key, value ):
		self.cache.append( [ copy.deepcopy(key), copy.deepcopy(value)] )
		if ( len(self.cache) > self.cacheMax ):
			self.flush()
	
	def close(self):
		self.flush()
		
	def flush(self):
		log("posting results: " + self.url)
		data = ""
		for out in self.cache:
			line = json.dumps( { 'id' : self.jobID, 'order' : self.order, 'key' : out[0] , 'value' : out[1] }  ) + "\n"
			data += line
			self.order += 1
			
		if (len(data)):
			urlopen(  self.url , data ).read()
		self.cache = []
	

	
class stdout_write:
	def __init__(self):
		self.order = 0

	def emit( self, key, value ):
		print self.order, key, value
		self.order += 1


def setWorkerID(id):
	global workerID
	workerID = id

global workerDict
workerDict = {}

def addWorker( type, callback ):
	global workerDict
	workerDict[ type ] = callback


def getWorker( host, applet ):
	global workerDict
	
	if workerDict.has_key( applet ):
		return workerDict[ applet ]

	appletDesc = httpGetJson( host + applet + "@pipeline" ).read()

	workerType = appletDesc[ 'codeType' ]
	if not workerDict.has_key( workerType ):
		raise Exception("Unknown code type: %s" % (workerType) )

	worker = workerDict[ workerType ]( appletDesc['mode'] )(host, applet)	
	worker.compileCode( appletDesc['code'] )

	if ( appletDesc.has_key( "output" ) ):
		worker.output = appletDesc[ "output" ]
	else:
		worker.output = []
		
	if ( appletDesc.has_key( "output" ) ):
		worker.setOutput( appletDesc[ "output" ] )
	
	if worker is not None:
		workerDict[ applet ] = worker
	return worker


import pythonWorker
