
import sys
import json
from cStringIO import StringIO
from urllib import urlopen

global remus_functions
global remus_server
global out_handle_map
global out_file_map

class PipeFileBuffer:
	def __init__(self, path):
		self.path = path
		self.buff = StringIO()
		
	def write(self, data):
		self.buff.write( data )
	
	def __repr__(self):
		return self.buff.getvalue()
		
	def close(self):
		pass
	

def open(path, mode="r"):
	global out_file_map
	if mode=="w":
		o = PipeFileBuffer(path)
		out_file_map[ path ] = o
		return o
	return urlopen( remus_server + path )
	
	
def mapper(f):
	return addFunction(f)

def reducer(f):
	return addFunction(f)

def pipe(f):
	return addFunction(f)

def merger(f):
	return addFunction(f)

def splitter(f):
	return addFunction(f)

def addFunction(f):
	global remus_functions
	remus_functions[ f.__module__ ] = f
	return f

def submit(applet, key, value ):
	instance = json.loads( urlopen( remus_server + "/@submit" ).read() )
	urlopen( remus_server + applet + "/@submit/" + instance, json.dumps( { key : value } ) ).read()
	return instance
	
def emit(key, val, output=None):
	global out_handle_map
	out_handle_map[ output ].emit( key, val )

def setoutput( outmap ):
	global file_outmap
	global out_file_map
	out_handle_map = outmap
	out_file_map = {}

def getoutput():
	global out_file_map
	return out_file_map

def getFunction(name):
	global out_handle_map
	return remus_functions[name]

def init(server):
	global remus_functions
	global out_handle_map
	global remus_server
	out_handle_map = {}
	remus_functions = {}
	out_handle = sys.stdout
	remus_server = server
