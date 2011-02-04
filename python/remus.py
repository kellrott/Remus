
import sys
import json
from urllib import urlopen

global remus_functions
global remus_server

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

def init(server):
	global remus_functions
	global out_handle_map
	global remus_server
	out_handle_map = {}
	remus_functions = {}
	out_handle = sys.stdout
	remus_server = server
