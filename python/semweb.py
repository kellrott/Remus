
import json
import sys
import inspect
import imp

global semweb_functions

def mapper(f):
	global semweb_functions
	semweb_functions[ f.__module__ ] = f
	return f

def reducer(f):
	global semweb_functions
	semweb_functions[ f.__module__ ] = f
	return f

def output(f):
	global semweb_functions
	semweb_functions[ f.__module__ ] = f
	return f


def merger(f):
	global semweb_functions
	semweb_functions[ f.__module__ ] = f
	return f

def splitter(f):
	global semweb_functions
	semweb_functions[ f.__module__ ] = f
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
	return semweb_functions[name]

def init():
	global semweb_functions
	global out_handle_map
	out_handle_map = {}
	semweb_functions = {}
	out_handle = sys.stdout
