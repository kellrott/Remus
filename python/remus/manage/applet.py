

from remus.fs.table import FSKeyTable

class AgentApplet(object):
	def __init__(self):
		pass


class PipeApplet(object):
	def __init__(self):
		self.created_tables = []
	
	def createTable(self, tableName):
		t = FSKeyTable(self.runInfo, tableName, True)
		self.created_tables.append(t)
		return t


class MapApplet(object):
	def __init__(self, inputTable):
		self.input = inputTable
	
	def run(self):
		for key, value in self.input:
			self.map(key, value)
