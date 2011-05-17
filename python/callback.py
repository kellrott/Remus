
import sys
import json


class RemusCallback:
	def __init__(self, parent ):
		self.parent = parent
		self.remus_functions = {}
		self.out_handle_map = {}
		self.out_file_list = []
		self.remus_functions = {}
		self.out_handle = sys.stdout

	def open(self, key, name, mode="r"):
		return self.parent.open( key, name, mode )		
		
	def mapper(self, f):
		return self.addFunction(f)
	
	def reducer(self, f):
		return self.addFunction(f)
	
	def pipe(self, f):
		return self.addFunction(f)
	
	def merger(self, f):
		return self.addFunction(f)
	
	def matcher(self, f):
		return self.addFunction(f)

	def agent(self, f):
		return self.addFunction(f)
	
	def splitter(self, f):
		return self.addFunction(f)
	
	def addFunction(self, f):
		self.remus_functions[ f.__module__ ] = f
		return f

	def emit(self, key, val, output=None):
		#print key, val, output, out_handle_map
		self.out_handle_map[ output ].emit( key, val )

	def setoutput( self, outmap ):
		self.out_handle_map = outmap
		self.out_file_list = []


	def getFunction(self, name):
		return self.remus_functions[name]
