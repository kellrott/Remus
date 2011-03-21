
import json
	
verbose = True

def log(v):
	"""
	Generic logging code, replace with something better later
	"""
	if ( verbose ):
		sys.stderr.write( v + "\n" )



def httpPostJson( url, data ):
	log( "posting:" + url )
	handle = urlopen( url, json.dumps(data) )
	return jsonIter( handle )

	
class httpStreamer:
	"""
	Class that reads off list of urls, opens each of the them sequentially
	then returns the results one line at a time	
	"""
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
	"""
	take handle, read one line at a time,
	parse to sequential json objects, and return them
	one at a time
	"""
	def __init__(self, handle):
		self.handle = handle
	
	def __iter__(self):
		try:
			for line in self.handle:
				yield json.loads( line )
		except ValueError:
			pass
		self.handle.close()
	
	def read(self):
		a = self.handle.read()
		return json.loads( a )

class valueIter:
	"""
	take jsonIter, but return only the first level values 
	in each object
	"""
	def __init__(self, handle):
		self.handle = handle
	
	def __iter__(self):
		for v in self.handle:
			for k in v:
				yield v[k]
	
class jsonPairSplitter:
	"""

	"""
	def __init__(self, iHandle):
		self.handle = iHandle
	
	def __iter__(self):
		for line in self.handle:
			data  = json.loads( line )
			for key in data:
				yield key, data[key]
	def close(self):
		pass


	
class stdout_write:
	def __init__(self):
		self.order = 0

	def emit( self, key, value ):
		print self.order, key, value
		self.order += 1



global workerDict
workerDict = {}

def addWorker( type, callback ):
	global workerDict
	workerDict[ type ] = callback


def getWorker( host, applet ):
	global workerDict
	
	if workerList.has_key( applet ):
		return workerList[ applet ]

	appletDesc = httpGetJson( host + applet + "@pipeline" ).read()

	workerType = appletDesc[ 'codeType' ]
	if not workerDict.has_key( workerType )
		raise Exception("Unknown code type: %s" % (workerType) )

	worker = workerDict[ workerType ]( appletDesc['mode'] )(host, applet)	
	worker.compileCode( appletDesc['code'] )
	
	if ( appletDesc.has_key( "output" ) ):
		worker.setOutput( appletDesc[ "output" ] )
	
	if worker is not None:
		workerList[ applet ] = worker
	return worker
