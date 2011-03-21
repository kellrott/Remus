
import remusLib


class pythonWorker


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
	def __init__(self, host, applet):
		self.host = host
		self.applet = applet
		pipeline = applet.split(":")[0]
		fileList = json.loads( urlopen( self.host + pipeline + "@attach" ).read() )
		for file in fileList:
			oHandle = open( file, "w" )
			fileURL =  self.host + pipeline + "@attach//" + file
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
			log( "ERROR: " + str(errorIDs) )
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

		errorIDs = {}
		for jobID in workDesc['input']:
			self.setupOutput(instance, jobID)
			inList = []
			for inFile in workDesc['input'][jobID]:
				kpURL = self.host + inFile
				log( "piping: " + kpURL )
				iHandle = jsonPairSplitter( urlopen( kpURL ) )
				inList.append( iHandle )
			try:
				func( inList )
			except Exception:
				exc_type, exc_value, exc_traceback = sys.exc_info()
				e = StringIO()
				traceback.print_exception(exc_type, exc_value, exc_traceback, file=e)				
				errorIDs[jobID ] = e.getvalue()
			self.closeOutput()		
			fileMap = remus.getoutput()
			for path in fileMap:
				postURL = self.host + self.applet + "@attach/%s/%s" % (instance, path)
				print postURL
				#print urlopen( postURL, fileMap[path].mem_map() ).read()
				#TODO, figure out streaming post in python
				fileMap[ path ].close()
				cmd = "curl --data-binary @%s %s" % (fileMap[ path ].getPath(), postURL )
				log( "OS: " + cmd )
				os.system( cmd )
				#fileMap[path].unlink()
			httpPostJson( self.host + self.applet + "@work", { instance : [ jobID ]  } )
		if ( len( errorIDs ) ):
			log( "ERROR: " + str(errorIDs) )
			httpPostJson( self.host + self.applet + "@error", { instance : errorIDs  } )



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



class MatchWorker(WorkerBase):	
	def doWork(self, instance, workDesc):		
		func = remus.getFunction( self.applet )
		errorIDs = {}
		doneIDs = []
		for jobID in workDesc['key']:
			log( "Starting Match %s %s" % (self.applet, jobID) )
			self.setupOutput(instance, jobID)
			wKey = workDesc['key'][jobID]
			leftValURL = self.host + workDesc['left_input'] + "/%s" % (  quote( wKey ) )
			leftSet = httpGetJson( leftValURL )
			rightValURL = self.host + workDesc['right_input'] + "/%s" % (  quote( wKey ) )
			rightSet = httpGetJson( rightValURL )
			func( wKey, valueIter(leftSet), valueIter(rightSet) )
			doneIDs.append( jobID )
			self.closeOutput()
		httpPostJson( self.host + self.applet + "@work", { instance : doneIDs } )
