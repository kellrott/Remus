
import json
import sys
import inspect
import imp

global remus_functions

def mapper(f):
	global remus_functions
	remus_functions[ f.__module__ ] = f
	return f

def reducer(f):
	global remus_functions
	remus_functions[ f.__module__ ] = f
	return f

def output(f):
	global remus_functions
	remus_functions[ f.__module__ ] = f
	return f


def merger(f):
	global remus_functions
	remus_functions[ f.__module__ ] = f
	return f

def splitter(f):
	global remus_functions
	remus_functions[ f.__module__ ] = f
	return f

global out_handle_map

def emit(key, val, output=None):
	global out_handle_map
	out_handle_map[ output ].write( "{\"%s\" : %s}\n" % (key, json.dumps(val)) )

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
