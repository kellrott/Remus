#!/usr/bin/env python

import remus
import sys
import json
from urllib  import quote
import imp
from cStringIO import StringIO
import uuid
from urlparse import urlparse
import httplib
import tempfile
import traceback
import os
import shutil
import threading

workerID = str(uuid.uuid4())

host = None
curServer = None
curConn = None
statusTimer = None

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

def statusPulse():
	log( "STATUS PULSE" )
	global host
	global statusTimer
	u = urlparse( host )
	conn = httplib.HTTPConnection(u.netloc)
	headers = {"Cookie":  'remusWorker=%s' % (workerID) }
	conn.request("GET", "/@status", None, headers)
	conn.close()
	statusTimer = threading.Timer(60, statusPulse)
	statusTimer.start()
	
	
verbose = True

def log(v):
	if ( verbose ):
		sys.stderr.write( v + "\n" )

class http_write:
	def __init__(self, url, jobID):
		self.url = url
		self.order = 0
		self.jobID = jobID
		self.cache = []
		
	def emit( self, key, value ):
		self.cache.append( [key, value] )
		if ( len(self.cache) > 1000 ):
			self.flush()
	
	def close(self):
		self.flush()
		
	def flush(self):
		log("posting results: " + self.url)
		data = ""
		for out in self.cache:
			data += json.dumps( { 'id' : self.jobID, 'order' : self.order, 'key' : out[0] , 'value' : out[1] }  ) + "\n"
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
	
class httpStreamer:
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

class jsonIter:
	def __init__(self, handle):
		self.handle = handle
	
	def __iter__(self):
		for line in self.handle:
			yield json.loads( line )
		self.handle.close()
	
	def read(self):
		a = self.handle.read()
		return json.loads( a )

class valueIter:
	def __init__(self, handle):
		self.handle = handle
	
	def __iter__(self):
		for v in self.handle:
			for k in v:
				yield v[k]
	
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
	log( "getting: " + url )
	if ( useCache ):
		if not getCache.has_key( url ):
			getCache[ url ] = urlopen( url ).read()			
		handle = StringIO(getCache[ url ])
	else:
		handle = urlopen( url )
	return jsonIter( handle )


def httpPostJson( url, data ):
	log( "posting:" + url )
	handle = urlopen( url, json.dumps(data) )
	return jsonIter( handle )


workerList = {}


def getWorker( host, applet ):
	if workerList.has_key( applet ):
		return workerList[ applet ]

	appletDesc = httpGetJson( host + applet + "@pipeline" ).read()
	worker = None
	if appletDesc['mode'] == 'split':
		worker = SplitWorker( host, applet )
	if appletDesc['mode'] == 'map':
		worker = MapWorker( host, applet )
	if appletDesc['mode'] == 'reduce':
		worker = ReduceWorker( host, applet )
	if appletDesc['mode'] == 'pipe':
		worker = PipeWorker( host, applet )
	if appletDesc['mode'] == 'merge':
		worker = MergeWorker( host, applet )
	worker.code = appletDesc['code']
	worker.compileCode()
	if ( appletDesc.has_key( "output" ) ):
		worker.output = appletDesc[ "output" ]
	else:
		worker.output = []
	
	if worker is not None:
		workerList[ applet ] = worker
	return worker

class WorkerBase:
	def __init__(self, host, applet):
		self.host = host
		self.applet = applet
		pipeline = applet.split(":")[0]
		fileList = json.loads( urlopen( self.host + pipeline + "@attach" ).read() )
		for file in fileList:
			oHandle = open( file, "w" )
			fileURL =  self.host + pipeline + "@attach///" + file
			oHandle.write( urlopen( fileURL ).read() )
			oHandle.close()
		
	def compileCode(self):
		self.module = imp.new_module( self.applet )	
		self.module.__dict__["__name__"] = self.applet
		exec self.code in self.module.__dict__

	def setupOutput(self, instance, jobID):
		outUrl = self.host + self.applet + "@data/%s" %(instance) 
		self.outmap = { None: http_write( outUrl, jobID ) }
		for outname in self.output:
			outUrl = self.host + self.applet + "." + outname + "@data/%s" % (instance)
			self.outmap[ outname ] = http_write( outUrl, jobID )
		remus.setoutput( self.outmap )
	
	def closeOutput(self):
		for name in self.outmap:
			self.outmap[name].close()
		self.outmap = []

class SplitWorker(WorkerBase):	
	def doWork(self, instance, workDesc):
		func = remus.getFunction( self.applet )
		doneIDs = []
		log( "Starting Split %s %s" % (self.applet, ",".join(workDesc['input'].keys()) ) )
		for jobID in workDesc['input']:
			self.setupOutput(instance, jobID)
			if ( workDesc[ 'input' ][jobID] is not None ):
				inputURL = self.host + workDesc[ 'input' ][jobID]
				iHandle = httpStreamer( [inputURL] )
			else:
				iHandle = None
			func( iHandle )	
			doneIDs.append( jobID )
		self.closeOutput()
		httpPostJson( self.host + self.applet + "@work", { instance : doneIDs  } )
		

class MapWorker(WorkerBase):	
	def doWork(self, instance, workDesc):
		func = remus.getFunction( self.applet )
		log( "Starting Map %s %s" % (self.applet, ",".join(workDesc['key'].keys()) ) )
		doneIDs = []
		errorIDs = {}
		for jobID in workDesc['key']:
			wKey = workDesc['key'][jobID]
			self.setupOutput(instance, jobID)
			kpURL = self.host + workDesc['input'] + "/%s" % ( quote( wKey ) )	
			kpData = httpGetJson( kpURL )
			try:
				for data in kpData:
					for key in data:
						func( key, data[key] )
				doneIDs.append( jobID )
			except Exception:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				e = StringIO()
				traceback.print_exception(exc_type, exc_value, exc_traceback, file=e)				
				errorIDs[jobID ] = e.getvalue()
		self.closeOutput()
		httpPostJson( self.host + self.applet + "@work", { instance : doneIDs  } )
		if ( len( errorIDs ) ):
			httpPostJson( self.host + self.applet + "@error", { instance : errorIDs  } )


class ReduceWorker(WorkerBase):	
	def doWork(self, instance, workDesc):
		func = remus.getFunction( self.applet )
		log( "Starting Reduce %s %s" % (self.applet, ",".join(workDesc['key'].keys())) )
		doneIDs = []
		for jobID in workDesc['key']:
			wKey = workDesc['key'][jobID]
			self.setupOutput(instance, jobID)
			kpURL = self.host + workDesc['input'] + "/%s" % ( quote( wKey ) )		
			kpData = httpGetJson( kpURL )
			func(  wKey, valueIter(kpData) )
			doneIDs.append( jobID )
		self.closeOutput()
		httpPostJson( self.host + self.applet + "@work", { instance : doneIDs  } )


class PipeWorker(WorkerBase):	
	def doWork(self, instance, workDesc):
		log( "Starting Pipe %s %s" % (self.applet, ",".join(workDesc['input'].keys()) ) )
		func = remus.getFunction( self.applet )
		for jobID in workDesc['input']:
			self.setupOutput(instance, jobID)
			inList = []
			for inFile in workDesc['input'][jobID]:
				kpURL = self.host + inFile
				log( "piping: " + kpURL )
				iHandle = jsonPairSplitter( urlopen( kpURL ) )
				inList.append( iHandle )
			func( inList )
			self.closeOutput()
		
			fileMap = remus.getoutput()
			for path in fileMap:
				postURL = self.host + self.applet + "@attach/%s//%s" % (instance, path)
				print postURL
				#print urlopen( postURL, fileMap[path].mem_map() ).read()
				#TODO, figure out streaming post in python
				cmd = "curl -d @%s %s" % (fileMap[ path ].getPath(), postURL )
				os.system( cmd )
				fileMap[path].unlink()
			httpPostJson( self.host + self.applet + "@work", { instance : [ jobID ]  } )


class MergeWorker(WorkerBase):	
	def doWork(self, instance, jobID):
		log( "Starting Merge %s %d" % (self.applet, jobID) )
		url = self.host + self.applet + "@work/%s/%s" % ( instance, jobID )
		jobSet = httpGetJson( url )
		func = remus.getFunction( self.applet )
		self.setupOutput(instance, jobID)
		for jobDesc in jobSet:
			leftValURL = self.host + jobDesc['left_input'] + "/%s" % (  quote( jobDesc['left_key']) )
			leftSet = httpGetJson( leftValURL )
			rightSetURL = self.host + jobDesc['right_input']
			for leftKey in leftSet:
				for rightSet in httpGetJson( rightSetURL, True ):
					for rightKey in rightSet:
						func( leftKey, leftVals[leftKey], rightKey, rightSet[rightKey] )
		self.closeOutput()
		httpPostJson( self.host + self.applet + "@work", { instance : [ jobID ]  } )


def doWork( host, applet, instance, workDesc ): 
	worker = getWorker( host, applet )
	if worker is not None:
		worker.doWork(instance, workDesc)
	
if __name__=="__main__":
	host = sys.argv[1]
	remus.init(host)
	tmpDir = tempfile.mkdtemp()
	os.chdir( tmpDir )
	sys.path.append( tmpDir )
	if ( len(sys.argv) >= 3 ):
		workerID = sys.argv[2]
	statusPulse()
	while 1:
		workList = httpGetJson( host + "/@work?max=100" ).read()	
		if len(workList) == 0:
			break
		for instance in workList:
			for node in workList[instance]:
				for workDesc in workList[instance][node]:
					doWork( host, node, instance, workDesc )
	shutil.rmtree( tmpDir ) 
	statusTimer.cancel()
