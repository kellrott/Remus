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
	def emit( self, key, value ):
		data = "key=%s&value=%s" % ( quote(json.dumps(key)), quote(json.dumps(value)) )
		print urlopen(  self.url, data ).read()
	

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
		inputURL = self.host + jobDesc[ 'input' ] 
		
		func = remus.getFunction( self.applet )
		iHandle = httpStreamer( [inputURL] )
		outURL =  self.host + self.applet + "@work?instance=%s&id=%s" % ( instance, jobID )
		remus.setoutput( { None: http_write( outURL ) } )
		func( iHandle )
	
		print httpPostJson( self.host + "/@work", { instance : { self.applet : [ jobID ] } } )
		
		
def doWork( host, applet, instance, jobID ): 
	worker = getWorker( host, applet )
	if worker is not None:
		worker.doWork(instance, jobID)
	
if __name__=="__main__":
	remus.init()
	host = "http://localhost:16016/"
	workList = httpGetJson( host + "/@work?max=1" )
	for instance in workList:
		for node in workList[instance]:
			for jobID in workList[instance][node]:
				doWork( host, node, instance, jobID )
	
