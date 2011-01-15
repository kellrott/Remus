
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

global out_handle

def emit(key, val):
	global out_handle
	out_handle.write( "{\"%s\" : %s}\n" % (key, json.dumps(val)) )


def setoutput( handle ):
	global out_handle
	out_handle = handle

def getFunction(name):
	global semweb_functions
	return semweb_functions[name]

def init():
	global semweb_functions
	semweb_functions = {}
	out_handle = sys.stdout
