
from urlparse import urlparse
from remusDB import AbstractStack, AbstractAttach
from remusLib import setStackDB, setAttachDB, log
import tempfile
import copy
from urllib2 import urlopen
import json
import httplib
from cStringIO import StringIO
import os

class HTTP_Stack( AbstractStack ):
	def __init__(self, server, workerID, pipeline, instance, applet ):
		AbstractStack.__init__(self, server, workerID, pipeline, instance, applet)
		self.curServer = None
		self.curConn = None
		self.workerID= workerID

		self.cache = []
		self.cacheMax = 10000

	def urlopen(self, url,data=None,retry=1):
		print "get", url
		u = urlparse( url )
		if self.curConn is None or self.curServer != u.netloc:
			self.curConn = httplib.HTTPConnection(u.netloc)
			self.curServer = u.netloc
		try:
			if data is not None:
				headers = {"Cookie":  'remusWorker=%s' % (self.workerID) }
				self.curConn.request("POST", u.path, data, headers)
				return StringIO( self.curConn.getresponse().read() )
			else:
				headers = {"Cookie":  'remusWorker=%s' % (self.workerID) }
				path = u.path
				if len(u.query):
					path += "?" + u.query
				self.curConn.request("GET", path, None, headers)
				return StringIO( self.curConn.getresponse().read() )
		except httplib.BadStatusLine:
			if retry > 0:
				self.curConn = None
				self.urlopen( url, data, retry-1)
	
	def get(self, key):
		path = "%s/%s/%s/%s/%s" % (self.server, self.pipeline, self.instance, self.applet, key)
		reqHandle = self.urlopen( path )
		for reqLine in reqHandle:
			data = json.loads( reqLine )
			for key in data:
				yield data[ key ]
		
	def put(self, key, jobID, emitID, value):
		self.cache.append( [ jobID, emitID, copy.deepcopy(key), copy.deepcopy(value)] )
		if ( len(self.cache) > self.cacheMax ):
			self.flush()
	
	def close(self):
		self.flush()
		if self.curConn is not None:
			self.curConn.close()
		
	def flush(self):
		url = "%s/%s/%s/%s" % (self.server, self.pipeline, self.instance, self.applet)
		log("posting results: " + url)
		data = ""
		for out in self.cache:
			line = json.dumps( { 'id' : out[0], 'order' : out[1], 'key' : out[2] , 'value' : out[3] }  ) + "\n"
			data += line
			
		if (len(data)):
			self.urlopen( url , data ).read()
		self.cache = []
	

	def listKVPairs(self):
		baseURL = "%s/%s/%s/%s" % (self.server, self.pipeline, self.instance, self.applet)
		handle = self.urlopen( baseURL )
		keyList = []
		for line in handle:
			keyList.append( json.loads( line ) )
		handle.close()
		for reqKey in keyList:
			reqHandle = self.urlopen( "%s/%s" % (baseURL, reqKey) )
			for reqLine in reqHandle:
				data = json.loads( reqLine )
				for key in data:
					yield key, data[key]




class OutFileBuffer:
	def __init__(self, url):
		self.url = url
		self.isOpen = True
		self.buff = tempfile.NamedTemporaryFile(delete=False)
		
	def write(self, data):
		self.buff.write( data )
		
	def close(self):
		if self.isOpen:
			self.buff.close()
			self.isOpen = False
			cmd = "curl -X PUT --data-binary @%s %s" % ( self.getPath(), self.url )
			print cmd
			os.system( cmd )
			self.unlink()
	
	def getPath(self):
		return self.buff.name
	
	def fileno(self):
		return self.buff.fileno()
	
	def unlink(self):
		os.unlink( self.buff.name )


class HTTP_Attach( AbstractAttach ):
	def open(self, key, name, mode = "r"):		
		url = self.server + "/" + self.pipeline + "/" + self.instance + \
				"/%s/%s/%s" % ( self.applet, key, name)				
		if mode == "r":
			return urlopen( url )
		elif mode == "w":				
			return OutFileBuffer( url )			

	
setStackDB( 'http', HTTP_Stack )
setAttachDB( 'http', HTTP_Attach )
