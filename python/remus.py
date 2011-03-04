
import sys
import json
from urllib import urlopen
import mmap
import tempfile
global remus_functions
global remus_server
global out_handle_map
global out_file_map

class PipeFileBuffer:
	def __init__(self, path):
		self.path = path
		self.buff = tempfile.NamedTemporaryFile(delete=False)
		
	def write(self, data):
		self.buff.write( data )
	
	def mem_map(self):
		mFile = mmap.mmap( self.buff.fileno(), 0, access=mmap.ACCESS_READ )
		return mFile
		
	def close(self):
		#self.buff.close()
		pass
	def unlink(self):
		self.buff.close()
		os.path.unlink( self.buff.name )

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

	
def emit(key, val, output=None):
	global out_handle_map
	#print key, val, output, out_handle_map
	out_handle_map[ output ].emit( key, val )

def setoutput( outmap ):
	global out_handle_map
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
