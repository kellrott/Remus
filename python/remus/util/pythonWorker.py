
import remusLib
import json
import imp

import callback
import sys
from cStringIO import StringIO
import traceback
import os
import mmap
import tempfile
import os
import copy
import urllib


def pythonWorker( mode ):
	if mode == 'split':
		return SplitWorker
	if mode == 'map':
		return MapWorker
	if mode == 'reduce':
		return ReduceWorker
	if mode == 'pipe':
		return PipeWorker
	if mode == 'merge':
		return MergeWorker
	if mode == 'match':
		return MatchWorker
	if mode == 'agent':
		return AgentWorker		
	return None


remusLib.addWorker( "python", pythonWorker )

class WorkerBase:
	def __init__(self, host, workerID, pipeline, instance, applet, appletDesc):
		self.host = host
		self.workerID = workerID
		self.appletDesc = appletDesc
		self.applet = applet
		self.instance = instance
		self.pipeline = pipeline
		self.useHTTP = False

		handle = remusLib.urlopen( self.host + self.pipeline + "/@attach" )
		for line in handle:
			fileName = json.loads( line ) 
			oHandle = open( fileName, "w" )
			fileURL =  self.host + self.pipeline + "/@attach/" + fileName
			oHandle.write( remusLib.urlopen( fileURL ).read() )
			oHandle.close()	

		self.compileCode( self.appletDesc['_code'] )
		self.func = self.callback.getFunction( self.applet )

		
	def compileCode(self, codePath):
		
		cHandle = open( codePath )
		code = cHandle.read()
		cHandle.close()
		
		remusLib.log("COMPILE:" + self.applet + "/" + self.instance )
		self.code = code
		self.callback = callback.RemusCallback( self )
		self.module = imp.new_module( self.applet )	
		self.module.__dict__["__file__"] = codePath		
		self.module.__dict__["__name__"] = self.applet
		self.module.__dict__["remus"] = self.callback
		exec self.code in self.module.__dict__

	def setupOutput(self, jobID):
		self.jobID = jobID
		self.emitID = 0
		outUrl = self.host + self.pipeline + "/" + self.instance + "/" + self.applet
		self.outputSet = { None : remusLib.StackWrapper( self.host, self.workerID, self.pipeline, self.instance, self.applet, useHTTP=self.useHTTP ) }
		if self.appletDesc.has_key( '_output' ):
			self.output = self.appletDesc[ "_output" ]
		else:
			self.output = []

		for outname in self.output:
			self.outputSet[ outname ] = remusLib.StackWrapper( self.host, self.workerID, self.pipeline, self.instance, "%s.%s" % (self.applet, outname), useHTTP=self.useHTTP ) 
		self.out_file_list = []
		
		if self.appletDesc.has_key( '_input' ) and isinstance( self.appletDesc['_input'], dict ) and self.appletDesc[ '_input' ].has_key('_applet'):
			self.inAttachReader = remusLib.AttachWrapper( self.host, self.workerID, self.pipeline, 
													self.appletDesc[ '_input' ]['_instance'],
													self.appletDesc[ '_input' ]['_applet'] )
		
		self.outAttachReader = remusLib.AttachWrapper( self.host, self.workerID, self.pipeline, 
													 self.instance, self.applet )
	
	def getStackDB( self, desc, major=True, applet=None, instance=None ):
		if applet is not None and instance is not None:
			return remusLib.StackWrapper( self.host, self.workerID, self.pipeline, instance, applet, useHTTP=self.useHTTP )
			
		if desc['_input'].has_key( '_axis' ):
			axis = desc['_input'][ '_axis' ]
			if not major:
				if axis == "_left":
					axis = "_right"
				else:
					axis = "_left"
			return remusLib.StackWrapper( self.host, self.workerID, self.pipeline, desc['_input'][axis]['_instance'], desc['_input'][axis]['_applet'] )
		return remusLib.StackWrapper( self.host, self.workerID, self.pipeline, desc['_input']['_instance'], desc['_input']['_applet'] )
	
	def closeOutput(self):
		for name in self.outputSet:
			self.outputSet[name].close()
		self.outmap = []		
		
	def doWork(self, jobID, jobKeys):
		mode = self.appletDesc[ '_mode' ]		

		doneIDs = []
		errorIDs = {}
		remusLib.log( "Starting Work %s %s" % (self.applet, jobID ) )

		doneIDs = []
		errorIDs = {}
	
		self.setupOutput(jobID)
		try:		
			self.work( self.func, self.appletDesc, jobKeys )
			doneIDs.append( jobID )
			
		except Exception:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				e = StringIO()
				traceback.print_exception(exc_type, exc_value, exc_traceback, file=e)				
				errorIDs[jobID ] = e.getvalue() 

		self.closeOutput()

		if ( len(doneIDs) ):
			remusLib.httpPostJson( self.host + "/@work", { self.instance : { "/" + self.pipeline + "/" + self.applet : doneIDs }  } )
		
		if ( len( errorIDs ) ):
			remusLib.log( "ERROR: " + str(errorIDs) )
			remusLib.httpPostJson( self.host + self.pipeline + "/" + self.instance + "/@error/" + self.applet ,  errorIDs  )


	def getAttachInputPath( self, desc, key, name ):
			return self.host + "/" + self.pipeline + "/" + desc['_input']['_instance'] + \
				"/%s/%s/%s" % ( desc['_input']['_applet'], key, name)

	def getAttachOutputPath( self, desc, key, name ):
			return self.host + "/" + self.pipeline + "/" + self.instance + \
				"/%s/%s/%s" % ( self.applet, key, name)
	
	def emit(self, key, value, name ):
		self.outputSet[ name ].put( key, self.jobID, self.emitID, value )
		self.emitID += 1
	
	def open(self, key, name, mode="r"):		
		if mode == 'r':
			return self.inAttachReader.open( key, name, mode )		
		if mode=="w":
			return self.outAttachReader.open( key, name, mode )

	def getInfo( self, name ):
		return self.appletDesc.get( name, None )
	
class SplitWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		func( appletDesc )
		

class MapWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		print appletDesc
		for key in keys:
			for dkey, data in remusLib.getDataStack( self.getStackDB( appletDesc), key=key ):
				func( dkey, data )


class ReduceWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		print appletDesc
		for key in keys:
			for dkey, data in remusLib.getDataStack( self.getStackDB( appletDesc ), key=key, reduce=True ):
				print "REDUCING", dkey, data
				func(  dkey, data )
		

class PipeWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		inList = []
		for inFile in appletDesc['_input']:
			remusLib.log( "piping: " + inFile["_instance"] + " " + inFile["_applet"] )
			iHandle = remusLib.getDataStack( self.getStackDB( appletDesc, applet=inFile['_applet'], instance=inFile['_instance'] ) ) 
			inList.append( iHandle )
		func( inList )


class MergeWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		for key in keys:
			majorURL = self.getStackDB( appletDesc )
			minorURL = self.getStackDB( appletDesc, False )				
			major = list( remusLib.getDataStack( majorURL, key=key, reduce=True, dataOnly=True ) )
			for mkey, mdata in remusLib.getDataStack( minorURL, reduce=True ):
				if appletDesc[ '_input' ][ '_axis' ] == '_left':			
					func( key, major[0], mkey, mdata )
				else:
					func( mkey, mdata, key, major[0] )


class MatchWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		for key in keys:
			majorURL = self.getStackDB( appletDesc )
			minorURL = self.getStackDB( appletDesc, False )	
			
			major = list( remusLib.getDataStack( majorURL, key=key, reduce=True, dataOnly=True ) )
			minor = list( remusLib.getDataStack( minorURL, key=key, reduce=True, dataOnly=True ) )
			if len( major ) and len( minor ):
				if appletDesc[ '_input' ][ '_axis' ] == '_left':			
					func( key, major[0], minor[0] )
				else:
					func( key, minor[0], major[0] )
				


class AgentWorker(WorkerBase):	
	
	def __init__(self, host, workerID, pipeline, instance, applet, appletDesc):
		WorkerBase.__init__(self, host, workerID, pipeline, instance, applet, appletDesc)
		self.useHTTP = True
		
	def work( self, func, appletDesc, keys ):
		print appletDesc
		for key in keys:
			for dkey, data in remusLib.getDataStack( self.getStackDB( appletDesc ), key=key ):
				func( dkey, data )
				
	def keylist( self, applet ):
		url = self.host + self.pipeline + "/" + self.instance + "/" + self.applet
		print "stack", url + "?info"
		infoData = remusLib.urlopen( url + "?info" ).read()
		print "DATA:", infoData
		info = json.loads( infoData )
		keyURL = self.host + info["_pipeline"] + "/" + applet.replace(":", "/")
		handle = remusLib.urlopen( keyURL )
		for line in handle:
			yield json.loads( line )

	def get( self, applet, key ):
		url = self.host + self.pipeline + "/" + self.instance + "/" + self.applet
		print "stack", url + "?info"
		info = json.loads( remusLib.urlopen( url + "?info" ).read() )
		keyURL = self.host + info["_pipeline"] + "/" + applet.replace(":", "/") + "/" + key
		handle = remusLib.urlopen( keyURL )
		for line in handle:
			yield json.loads( line )[ key ]

