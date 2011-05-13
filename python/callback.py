
import sys
import json
from urllib import urlopen
import mmap
import tempfile
import os
import copy

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


class RemusCallback:
	def __init__(self, server, pipeline, applet, appletDesc):
		self.server = server
		self.pipeline = pipeline
		self.applet = applet
		self.appletDesc = appletDesc
		self.remus_functions = {}
		self.out_handle_map = {}
		self.out_file_list = []
		self.remus_functions = {}
		self.out_handle = sys.stdout
		self.remus_server = server

	def open(self, key, name, mode="r"):
		if mode=="w":
			o = PipeFileBuffer(key, name)
			self.out_file_list.append( [key, name, o] )
			return o
		attachPath = "%s/%s/%s/%s/%s/%s" % (self.remus_server, self.pipeline, self.appletDesc['_input']['_instance'], self.appletDesc['_input']['_applet'], key, name )
		print "GETTING: " + attachPath
		return urlopen( attachPath ) 
		
		
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
	
	def getoutput(self):
		return self.out_file_list
	
	def getFunction(self, name):
		return self.remus_functions[name]
