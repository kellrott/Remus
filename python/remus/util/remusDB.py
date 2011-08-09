

class AbstractStack:
	def __init__(self, server, workerID, pipeline, instance, applet ):
		self.server = server
		self.pipeline = pipeline
		self.instance = instance
		self.applet = applet
	
	def get(self, key):
		raise Exception()
		
	def put(self, key, jobID, emitID, value):
		raise Exception()

	def listKVPairs(self):
		raise Exception()

	def close(self):
		raise Exception()


class AbstractAttach:
	def __init__(self, server, workerID, pipeline, instance, applet ):
		self.server = server
		self.pipeline = pipeline
		self.instance = instance
		self.applet = applet
	
	def open(self, key, name, mode):
		raise Exception()
		
