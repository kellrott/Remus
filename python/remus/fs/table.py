
import os
import json

class FSKeyTable(object):
	def __init__(self, runInfo, name, create=False):
		self.path = os.path.join(runInfo['fsBase'], runInfo["_pipeline"], runInfo["_instance"], name)
		if create:
			if not os.path.exists(os.path.dirname(self.path)):
				os.makedirs(os.path.dirname(self.path))
			if os.path.exists(self.path):
				raise Exception("Writing to existing table")
			self.handle = open(self.path, "w")
		self.name = name
		self.runInfo = runInfo

	def close(self):
		self.handle.close()
		self.handle = None

	def __iter__(self):
		if self.handle is not None:
			raise Exception("Reading from open table")
			
		self.handle = open(self.path)
		for line in self.handle:
			tmp = line.split("\t")
			yield tmp[0], json.loads(tmp[1])
		self.handle.close()
		self.handle = None
			
	def emit(self, key, value):
		self.handle.write( "%s\t%s\n" % (key, json.dumps(value)))
	
	def attach(self, path, key, name):
		print "attach", self.name, path, key, name
