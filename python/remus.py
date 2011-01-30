
import sys
import inspect
import imp

global remus_functions

def mapper(f):
	return addFunction(f)

def reducer(f):
	return addFunction(f)

def output(f):
	return addFunction(f)

def merger(f):
	return addFunction(f)

def splitter(f):
	return addFunction(f)

def addFunction(f):
	global remus_functions
	remus_functions[ f.__module__ ] = f
	return f

global out_handle_map

def emit(key, val, output=None):
	global out_handle_map
	out_handle_map[ output ].emit( key, val )

def setoutput( outmap ):
	global out_handle_map
	out_handle_map = outmap

def getFunction(name):
	global out_handle_map
	return remus_functions[name]

def init():
	global remus_functions
	global out_handle_map
	out_handle_map = {}
	remus_functions = {}
	out_handle = sys.stdout
