
import remusLib
import json
import imp
import remus
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


class PipeFileBuffer:
	def __init__(self, key, name):
		self.key = key
		self.name = name
		self.isOpen = True
		self.buff = tempfile.NamedTemporaryFile(delete=False)
		
	def write(self, data):
		self.buff.write( data )
	
	def mem_map(self):
		mFile = mmap.mmap( self.buff.fileno(), 0, access=mmap.ACCESS_READ )
		return mFile
		
	def close(self):
		if self.isOpen:
			self.buff.close()
		self.isOpen = False
	
	def getPath(self):
		return self.buff.name
	
	def fileno(self):
		return self.buff.fileno()
	
	def unlink(self):
		os.unlink( self.buff.name )




remusLib.addWorker( "python", pythonWorker )

class WorkerBase:
	def __init__(self, host, pipeline, instance, applet, appletDesc):
		self.host = host
		self.appletDesc = appletDesc
		self.applet = applet
		self.instance = instance
		self.pipeline = pipeline

		handle = remusLib.urlopen( self.host + self.pipeline + "/@attach" )
		for line in handle:
			fileName = json.loads( line ) 
			oHandle = open( fileName, "w" )
			fileURL =  self.host + self.pipeline + "/@attach/" + fileName
			oHandle.write( remusLib.urlopen( fileURL ).read() )
			oHandle.close()	

		self.compileCode( self.appletDesc['code'] )
		self.func = self.callback.getFunction( self.applet )

		
	def compileCode(self, code):
		remusLib.log("COMPILE:" + self.applet + "/" + self.instance )
		self.code = code
		self.callback = callback.RemusCallback( self )
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
		self.out_file_list = []
		
	def closeOutput(self):
		for name in self.outmap:
			self.outmap[name].close()
		self.outmap = []

		for key, name, handle in self.out_file_list:
			postURL = self.getAttachOutputPath( self.appletDesc, key, name ) 
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

	def getAttachInputPath( self, desc, key, name ):
			return self.host + "/" + self.pipeline + "/" + desc['_input']['_instance'] + \
				"/%s/%s/%s" % ( desc['_input']['_applet'], key, name)

	def getAttachOutputPath( self, desc, key, name ):
			return self.host + "/" + self.pipeline + "/" + self.instance + \
				"/%s/%s/%s" % ( self.applet, key, name)
	
	def open(self, key, name, mode="r"):
		if mode=="w":
			o = PipeFileBuffer(key, name)
			self.out_file_list.append( [key, name, o] )
			return o
		attachPath = "%s/%s/%s/%s/%s/%s" % (self.host, self.pipeline, self.appletDesc['_input']['_instance'], self.appletDesc['_input']['_applet'], key, name )
		print "GETTING: " + attachPath
		return urllib.urlopen( attachPath ) 
		
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
				


class AgentWorker(WorkerBase):	
	def work( self, func, appletDesc, keys ):
		print appletDesc
		for key in keys:
			for dkey, data in remusLib.getDataStack( self.getInputPath( appletDesc), key=key ):
				func( dkey, data )
				

