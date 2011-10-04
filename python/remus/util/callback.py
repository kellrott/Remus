
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
	
	def copyTo(self, path, key, name):
		return self.parent.copyTo(path, key, name) 
		
	def map(self, f):
		return self.addFunction(f)
	
	def reduce(self, f):
		return self.addFunction(f)
	
	def pipe(self, f):
		return self.addFunction(f)
	
	def merge(self, f):
		return self.addFunction(f)
	
	def match(self, f):
		return self.addFunction(f)

	def agent(self, f):
		return self.addFunction(f)
	
	def split(self, f):
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
	
	def getInfo(self, name):
		return self.parent.getInfo( name )
