
import remusLib
import json
import imp
import remus
import callback
import sys
from cStringIO import StringIO
import traceback


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
	def __init__(self, host, pipeline, applet):
		self.host = host
		self.applet = applet
		self.pipeline = pipeline
		fileList = json.loads( remusLib.urlopen( self.host + self.pipeline + "/@attach" ).read() )
		for file in fileList:
			oHandle = open( file, "w" )
			fileURL =  self.host + self.pipeline + "/@attach/" + file
			oHandle.write( remusLib.urlopen( fileURL ).read() )
			oHandle.close()	
		
	def compileCode(self, code):
		self.code = code
		self.callback = callback.RemusCallback( self.host, self.pipeline, self.applet )
		self.module = imp.new_module( self.applet )	
		self.module.__dict__["__name__"] = self.applet
		self.module.__dict__["remus"] = self.callback
		exec self.code in self.module.__dict__

	def setupOutput(self, instance, jobID):
		outUrl = self.host + self.pipeline + "/" + self.applet + "@data/%s" %(instance) 
		self.outmap = { None: remusLib.http_write( outUrl, jobID ) }
		for outname in self.output:
			outUrl = self.host + self.applet + "." + outname + "@data/%s" % (instance)
			self.outmap[ outname ] = http_write( outUrl, jobID )
		self.callback.setoutput( self.outmap )
	
	def closeOutput(self):
		for name in self.outmap:
			self.outmap[name].close()
		self.outmap = []

class SplitWorker(WorkerBase):	
	def doWork(self, instance, workDesc):
		func = self.callback.getFunction( self.applet )
		doneIDs = []
		remusLib.log( "Starting Split %s %s" % (self.applet, ",".join(workDesc['input'].keys()) ) )
		for jobID in workDesc['input']:
			self.setupOutput(instance, jobID)
			if ( workDesc[ 'input' ][jobID] is not None ):
				inputURL = self.host + workDesc[ 'input' ][jobID]
				print "Desc",  workDesc[ 'input' ][jobID]
				iHandle = remusLib.httpStreamer( [inputURL] )
			else:
				iHandle = None
			func( iHandle )	
			doneIDs.append( jobID )
			self.closeOutput()
		remusLib.httpPostJson( self.host + self.pipeline + "/" + self.applet + "@work", { instance : doneIDs  } )
		

class MapWorker(WorkerBase):	
	def doWork(self, instance, workDesc):
		func = self.callback.getFunction( self.applet )
		remusLib.log( "Starting Map %s %s" % (self.applet, ",".join(workDesc['key'].keys()) ) )
		doneIDs = []
		errorIDs = {}
		for jobID in workDesc['key']:
			wKey = workDesc['key'][jobID]
			self.setupOutput(instance, jobID)
			kpURL = self.host + workDesc['input'] + "/%s" % ( remusLib.quote( wKey ) )	
			kpData = remusLib.httpGetJson( kpURL )
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
		remusLib.httpPostJson( self.host + self.pipeline + "/" + self.applet + "@work", { instance : doneIDs  } )
		if ( len( errorIDs ) ):
			remusLib.log( "ERROR: " + str(errorIDs) )
			remusLib.httpPostJson( self.host + self.pipeline + "/" + self.applet + "@error", { instance : errorIDs  } )


class ReduceWorker(WorkerBase):	
	def doWork(self, instance, workDesc):
		func = self.callback.getFunction( self.applet )
		remusLib.log( "Starting Reduce %s %s" % (self.applet, ",".join(workDesc['key'].keys())) )
		doneIDs = []
		errorIDs = {}
		for jobID in workDesc['key']:
			wKey = workDesc['key'][jobID]
			self.setupOutput(instance, jobID)
			kpURL = self.host + workDesc['input'] + "/%s" % ( remusLib.quote( wKey ) )		
			kpData = remusLib.httpGetJson( kpURL )
			try:
				func(  wKey, remusLib.valueIter(kpData) )
				doneIDs.append( jobID )
			except Exception:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				e = StringIO()
				traceback.print_exception(exc_type, exc_value, exc_traceback, file=e)				
				errorIDs[jobID ] = e.getvalue()				
			self.closeOutput()
		if len( doneIDs ):
			remusLib.httpPostJson( self.host + self.pipeline + "/" + self.applet + "@work", { instance : doneIDs  } )
		if ( len( errorIDs ) ):
			remusLib.log( "ERROR: " + str(errorIDs) )
			remusLib.httpPostJson( self.host + self.pipeline + "/" + self.applet + "@error", { instance : errorIDs  } )

class PipeWorker(WorkerBase):	
	def doWork(self, instance, workDesc):
		remusLib.log( "Starting Pipe %s %s" % (self.applet, ",".join(workDesc['input'].keys()) ) )
		func = self.callback.getFunction( self.applet )

		errorIDs = {}
		for jobID in workDesc['input']:
			self.setupOutput(instance, jobID)
			inList = []
			for inFile in workDesc['input'][jobID]:
				kpURL = self.host + inFile
				remusLib.log( "piping: " + kpURL )
				iHandle = remusLib.jsonPairSplitter( remusLib.urlopen( kpURL ) )
				inList.append( iHandle )
			try:
				func( inList )
			except Exception:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				e = StringIO()
				traceback.print_exception(exc_type, exc_value, exc_traceback, file=e)				
				errorIDs[jobID ] = e.getvalue()
			self.closeOutput()		
			fileMap = self.callback.getoutput()
			for path in fileMap:
				postURL = self.host + self.pipeline + "/" + self.applet + "@attach/%s/%s" % (instance, path)
				print postURL
				#print urlopen( postURL, fileMap[path].mem_map() ).read()
				#TODO, figure out streaming post in python
				fileMap[ path ].close()
				cmd = "curl --data-binary @%s %s" % (fileMap[ path ].getPath(), postURL )
				remusLib.log( "OS: " + cmd )
				os.system( cmd )
				#fileMap[path].unlink()
			remusLib.httpPostJson( self.host + self.pipeline + "/" + self.applet + "@work", { instance : [ jobID ]  } )
		if ( len( errorIDs ) ):
			remusLib.log( "ERROR: " + str(errorIDs) )
			remusLib.httpPostJson( self.host + self.pipeline + "/" + self.applet + "@error", { instance : errorIDs  } )



class MergeWorker(WorkerBase):	
	def doWork(self, instance, workDesc):
		func = self.callback.getFunction( self.applet )
		errorIDs = {}
		doneIDs = []
		for jobID in workDesc['left_key']:
			url = self.host + self.applet + "@work/%s/%s" % ( instance, jobID )
			self.setupOutput(instance, jobID)

			leftKey = workDesc['left_key'][jobID]
			leftValURL = self.host + workDesc['left_input'] + "/%s" % (  remusLib.quote( leftKey ) )
			rightSetURL = self.host + workDesc['right_input']
			
			leftSet = list( remusLib.httpGetJson( leftValURL ) )
			
			rightKey = None
			rightSet = []
			for data in remusLib.httpGetJson( rightSetURL ):
				for key in data:
					if rightKey is None:
						rightKey = key
					if key != rightKey:
						func( leftKey, remusLib.valueIter(leftSet), rightKey, remusLib.valueIter(rightSet) )
						rightKey = key
						rightSet = []
					rightSet.append( { key : data[key] } )
			if len( rightSet):
				func( leftKey, remusLib.valueIter(leftSet), rightKey, remusLib.valueIter(rightSet) )
			self.closeOutput()					
			remusLib.httpPostJson( self.host + self.pipeline + "/" + self.applet + "@work", { instance : [ jobID ]  } )



class MatchWorker(WorkerBase):	
	def doWork(self, instance, workDesc):		
		func = self.callback.getFunction( self.applet )
		errorIDs = {}
		doneIDs = []
		for jobID in workDesc['key']:
			remusLib.log( "Starting Match %s %s" % (self.applet, jobID) )
			self.setupOutput(instance, jobID)
			wKey = workDesc['key'][jobID]
			leftValURL = self.host + workDesc['left_input'] + "/%s" % (  remusLib.quote( wKey ) )
			leftSet = remusLib.httpGetJson( leftValURL )
			rightValURL = self.host + workDesc['right_input'] + "/%s" % (  remusLib.quote( wKey ) )
			rightSet = remusLib.httpGetJson( rightValURL )
			func( wKey, remusLib.valueIter(leftSet), remusLib.valueIter(rightSet) )
			doneIDs.append( jobID )
			self.closeOutput()
		remusLib.httpPostJson( self.host + self.pipeline + "/" + self.applet + "@work", { instance : doneIDs } )
