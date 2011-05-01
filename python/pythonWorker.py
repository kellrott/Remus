
import remusLib
import json
import imp
import remus
import callback
import sys
from cStringIO import StringIO
import traceback
import os

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
	return None


remusLib.addWorker( "python", pythonWorker )

class WorkerBase:
	def __init__(self, host, pipeline, instance, applet, appletDesc):
		self.host = host
		self.appletDesc = appletDesc
		self.applet = applet
		self.instance = instance
		self.pipeline = pipeline

		fileList = json.loads( remusLib.urlopen( self.host + self.pipeline + "/@attach" ).read() )
		for file in fileList:
			oHandle = open( file, "w" )
			fileURL =  self.host + self.pipeline + "/@attach/" + file
			oHandle.write( remusLib.urlopen( fileURL ).read() )
			oHandle.close()	

		self.compileCode( self.appletDesc['code'] )
		self.func = self.callback.getFunction( self.applet )

		
	def compileCode(self, code):
		remusLib.log("COMPILE:" + self.applet + "/" + self.instance )
		self.code = code
		self.callback = callback.RemusCallback( self.host, self.pipeline, self.applet )
		self.module = imp.new_module( self.applet )	
		self.module.__dict__["__name__"] = self.applet
		self.module.__dict__["remus"] = self.callback
		exec self.code in self.module.__dict__

	def setupOutput(self, jobID):
		outUrl = self.host + self.pipeline + "/" + self.instance + "/" + self.applet    
		self.outmap = { None: remusLib.http_write( outUrl, jobID ) }
		for outname in self.output:
			outUrl = self.host + "/" + self.pipeline + "/" + self.instance + self.applet + "." + outname
			self.outmap[ outname ] = http_write( outUrl, jobID )
		self.callback.setoutput( self.outmap )
	
	def closeOutput(self):
		for name in self.outmap:
			self.outmap[name].close()
		self.outmap = []

		fileMap = self.callback.getoutput()
		for key, name, handle in fileMap:
			postURL = self.host + self.pipeline + "/" + instance + "/%s/%s/%s" % (self.applet, key, name)
			print "ATTACHMENT:", postURL
			#print urlopen( postURL, fileMap[path].mem_map() ).read()
			#TODO, figure out streaming post in python
			handle.close()
			cmd = "curl -X PUT --data-binary @%s %s" % ( handle.getPath(), postURL )
			remusLib.log( "OS: " + cmd )
			os.system( cmd )
			handle.unlink()
			#fileMap[path].unlink()

	def doWork(self, jobID, jobKeys):
		mode = self.appletDesc[ 'mode' ]		

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

	def getInputPath( self, desc, major=True ):
		if desc['_input'].has_key( '_axis' ):
			axis = desc['_input'][ '_axis' ]
			if not major:
				if axis == "_left":
					axis = "_right"
				else:
					axis = "_left"
			return self.host + "/" + self.pipeline + "/" + desc['_input'][axis]['_instance'] + "/" + desc['_input'][axis]['_applet']
		return self.host + "/" + self.pipeline + "/" + desc['_input']['_instance'] + "/" + desc['_input']['_applet']


class SplitWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		func( appletDesc )
		

class MapWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		print appletDesc
		for key in keys:
			for dkey, data in remusLib.getDataStack( self.getInputPath( appletDesc), key=key ):
				func( dkey, data )


class ReduceWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		print appletDesc
		for key in keys:
			for dkey, data in remusLib.getDataStack( self.getInputPath( appletDesc ), key=key, reduce=True ):
				print "REDUCING", dkey, data
				func(  dkey, data )
		

class PipeWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		inList = []
		for inFile in keys[0]: #appletDesc['input']:
			kpURL = self.host + inFile
			remusLib.log( "piping: " + kpURL )
			iHandle = remusLib.getDataStack( kpURL ) 
			inList.append( iHandle )
		func( inList )


class MergeWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		for key in keys:
			majorURL = self.getInputPath( appletDesc )
			minorURL = self.getInputPath( appletDesc, False )				
			major = list( remusLib.getDataStack( majorURL, key=key, reduce=True, dataOnly=True ) )
			for mkey, mdata in remusLib.getDataStack( minorURL, reduce=True ):
				if appletDesc[ '_input' ][ '_axis' ] == '_left':			
					func( key, major[0], mkey, mdata )
				else:
					func( mkey, mdata, key, major[0] )


class MatchWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		for key in keys:
			majorURL = self.getInputPath( appletDesc )
			minorURL = self.getInputPath( appletDesc, False )	
			
			major = list( remusLib.getDataStack( majorURL, key=key, reduce=True, dataOnly=True ) )
			minor = list( remusLib.getDataStack( minorURL, key=key, reduce=True, dataOnly=True ) )
			if len( major ) and len( minor ):
				if appletDesc[ '_input' ][ '_axis' ] == '_left':			
					func( key, major[0], minor[0] )
				else:
					func( key, minor[0], major[0] )
				