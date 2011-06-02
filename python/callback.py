
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
		self.parent.emit( key, val, output )

	def keylist( self, instance ):
		return self.parent.keylist( instance )

	def get( self, instance, key ):
		return self.parent.get( instance, key )

	def getFunction(self, name):
		return self.remus_functions[name]
