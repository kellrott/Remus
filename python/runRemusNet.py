#!/usr/bin/env python

import remus
import sys
import json
from urllib import urlopen, quote
import remus
import imp

class http_write:
	def __init__(self, url):
		self.url = url
		self.order = 0
	def emit( self, key, value ):
		data = "order=%d&key=%s&value=%s" % ( self.order, quote(json.dumps(key)), quote(json.dumps(value)) )
		self.order += 1
		print urlopen(  self.url, data ).read()

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

def httpGetJson( url ):
	handle = urlopen( url )
	o = json.loads( handle.read() )
	handle.close()
	return o


def httpPostJson( url, data ):
	handle = urlopen( url, json.dumps(data) )
	o = json.loads( handle.read() )
	handle.close()
	return o


workerList = {}


def getWorker( host, applet ):
	if workerList.has_key( applet ):
		return workerList[ applet ]

	appletDesc = httpGetJson( host + applet )
	worker = None
	if appletDesc['mode'] == 'split':
		worker = SplitWorker( host, applet )
	if appletDesc['mode'] == 'map':
		worker = MapWorker( host, applet )
	worker.getCode()
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


class SplitWorker(WorkerBase):	
	def doWork(self, instance, jobID):
		url = self.host + self.applet + "@work?instance=%s&id=%s" % ( instance, jobID )
		jobDesc = httpGetJson( url )
		func = remus.getFunction( self.applet )
		if ( jobDesc[ 'input' ] is not None ):
			inputURL = self.host + jobDesc[ 'input' ] 
			iHandle = httpStreamer( [inputURL] )
		else:
			iHandle = None
		outURL =  self.host + self.applet + "@work?instance=%s&id=%s" % ( instance, jobID )
		remus.setoutput( { None: http_write( outURL ) } )
		func( iHandle )
	
		print httpPostJson( self.host + "/@work", { instance : { self.applet : [ jobID ] } } )
		

class MapWorker(WorkerBase):	
	def doWork(self, instance, jobID):
		url = self.host + self.applet + "@work?instance=%s&id=%s" % ( instance, jobID )
		jobDesc = httpGetJson( url )
		kpURL = self.host + jobDesc['input'] + "@data?instance=%s&jobID=%d&emitID=%d" % ( instance, jobDesc['jobID'], jobDesc['emitID'] )
		print kpURL
		func = remus.getFunction( self.applet )
		
		kpData = httpGetJson( kpURL )
		
		outURL =  self.host + self.applet + "@work?instance=%s&id=%s" % ( instance, jobID )
		remus.setoutput( { None: http_write( outURL ) } )
		for key in kpData:
			func( key, kpData[ key ] )
		print "jobid", jobID
		print httpPostJson( self.host + "/@work", { instance : { self.applet : [ jobID ] } } )
		
def doWork( host, applet, instance, jobID ): 
	worker = getWorker( host, applet )
	if worker is not None:
		worker.doWork(instance, jobID)
	
if __name__=="__main__":
	remus.init()
	host = "http://localhost:16016/"
	workList = httpGetJson( host + "/@work?max=100" )
	for instance in workList:
		for node in workList[instance]:
			for jobID in workList[instance][node]:
				doWork( host, node, instance, jobID )
	
