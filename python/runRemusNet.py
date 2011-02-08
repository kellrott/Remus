#!/usr/bin/env python

import remus
import sys
import json
from urllib2 import urlopen
from urllib  import quote
import remus
import imp
from cStringIO import StringIO

verbose = True

def log(v):
	if ( verbose ):
		sys.stderr.write( v + "\n" )

class http_write:
	def __init__(self, url, instance, jobID):
		self.url = url
		self.order = 0
		self.jobID = jobID
		self.instance = instance
		self.cache = []
		
	def emit( self, key, value ):
		self.cache.append( [key, value] )
		if ( len(self.cache) > 1000 ):
			self.flush()
	
	def close(self):
		self.flush()
		
	def flush(self):
		data = ""
		for out in self.cache:
			data += json.dumps( { 'instance' : self.instance, 'id' : self.jobID, 'order' : self.order, 'key' : out[0] , 'value' : out[1] }  ) + "\n"
			self.order += 1
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
		print url
		handle = urlopen( url )
	return jsonIter( handle )


def httpPostJson( url, data ):
	print url
	handle = urlopen( url, json.dumps(data) )
	return jsonIter( handle )


workerList = {}


def getWorker( host, applet ):
	if workerList.has_key( applet ):
		return workerList[ applet ]

	appletDesc = httpGetJson( host + applet + "@info" ).read()
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
	worker.getCode()
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
		
	def getCode(self):
		self.code = urlopen( self.host + self.applet + "@code" ).read()
		self.module = imp.new_module( self.applet )	
		self.module.__dict__["__name__"] = self.applet
		exec self.code in self.module.__dict__

	def setupOutput(self, instance, jobID):
		outUrl = self.host + self.applet + "@data" 
		self.outmap = { None: http_write( outUrl, instance, jobID ) }
		for outname in self.output:
			outUrl = self.host + self.applet + "." + outname + "@data"
			self.outmap[ outname ] = http_write( outUrl, instance, jobID )
		remus.setoutput( self.outmap )
	
	def closeOutput(self):
		for name in self.outmap:
			self.outmap[name].close()
		self.outmap = []

class SplitWorker(WorkerBase):	
	def doWork(self, instance, jobID):
		log( "Starting Split %s %d" % (self.applet, jobID) )
		url = self.host + self.applet + "@work/%s/%s" % ( instance, jobID )
		jobSet = httpGetJson( url )
		func = remus.getFunction( self.applet )
		self.setupOutput(instance, jobID)
		for jobDesc in jobSet:
			if ( jobDesc[ 'input' ] is not None ):
				inputURL = self.host + jobDesc[ 'input' ] 
				iHandle = httpStreamer( [inputURL] )
			else:
				iHandle = None
			func( iHandle )	
		self.closeOutput()
		httpPostJson( self.host + self.applet + "@work", { instance : [ jobID ]  } )
		

class MapWorker(WorkerBase):	
	def doWork(self, instance, jobID):
		log( "Starting Map %s %d" % (self.applet, jobID) )
		url = self.host + self.applet + "@work/%s/%s" % ( instance, jobID )
		func = remus.getFunction( self.applet )
		self.setupOutput(instance, jobID)
		jobSet = httpGetJson( url )
		for jobDesc in jobSet:
			kpURL = self.host + jobDesc['input'] + "/%s/%s" % ( instance, quote(json.dumps(jobDesc['key'])) )	
			kpData = httpGetJson( kpURL )
			for data in kpData:
				func( jobDesc['key'], data )
		self.closeOutput()
		httpPostJson( self.host + self.applet + "@work", { instance : [ jobID ]  } )

class ReduceWorker(WorkerBase):	
	def doWork(self, instance, jobID):
		log( "Starting Reduce %s %d" % (self.applet, jobID) )
		url = self.host + self.applet + "@work/%s/%s" % ( instance, jobID )
		jobSet = httpGetJson( url )
		func = remus.getFunction( self.applet )
		self.setupOutput(instance, jobID)
		for jobDesc in jobSet:
			kpURL = self.host + jobDesc['input'] + "/%s/%s" % ( instance, quote(json.dumps(jobDesc['key'])) )		
			kpData = httpGetJson( kpURL )
			func( jobDesc['key'], kpData )
		self.closeOutput()
		httpPostJson( self.host + self.applet + "@work", { instance : [ jobID ]  } )


class PipeWorker(WorkerBase):	
	def doWork(self, instance, jobID):
		log( "Starting Pipe %s %d" % (self.applet, jobID) )
		url = self.host + self.applet + "@work/%s/%s" % ( instance, jobID )
		jobDesc = httpGetJson( url ).read()
		func = remus.getFunction( self.applet )
		self.setupOutput(instance, jobID)
		inList = []
		for inFile in jobDesc['input']:
			kpURL = self.host + inFile + "/%s" % ( instance )
			print kpURL
			iHandle = jsonPairSplitter( urlopen( kpURL ) )
			inList.append( iHandle )
		func( inList )
		self.closeOutput()
		httpPostJson( self.host + self.applet + "@work", { instance : [ jobID ]  } )


class MergeWorker(WorkerBase):	
	def doWork(self, instance, jobID):
		log( "Starting Merge %s %d" % (self.applet, jobID) )
		url = self.host + self.applet + "@work/%s/%s" % ( instance, jobID )
		jobSet = httpGetJson( url )
		func = remus.getFunction( self.applet )
		self.setupOutput(instance, jobID)
		for jobDesc in jobSet:
			leftKey = jobDesc['left_key']
			leftValURL = self.host + jobDesc['left_input'] + "/%s/%s" % ( instance, quote(json.dumps(jobDesc['left_key'])) )
			leftVals = list( httpGetJson( leftValURL ) )
			rightSetURL = self.host + jobDesc['right_input'] + "@reduce/%s" % ( instance )
			for rightSet in httpGetJson( rightSetURL, True ):
				for rightKey in rightSet:
					func( leftKey, leftVals, rightKey, rightSet[rightKey] )
		self.closeOutput()
		httpPostJson( self.host + self.applet + "@work", { instance : [ jobID ]  } )


def doWork( host, applet, instance, jobID ): 
	worker = getWorker( host, applet )
	if worker is not None:
		worker.doWork(instance, jobID)
	
if __name__=="__main__":
	host = "http://localhost:16016/"
	remus.init(host)
	while 1:
		workList = httpGetJson( host + "/@work?max=100" ).read()	
		if len(workList) == 0:
			break
		for instance in workList:		
			for node in workList[instance]:
				for jobID in workList[instance][node]:
					doWork( host, node, instance, jobID )
	
