#!/usr/bin/env python

import json
import sys
from xml.dom.minidom import parseString
import os

def getText(nodelist):
    rc = []
    for node in nodelist:
        #if node.nodeType == node.TEXT_NODE:
        rc.append(node.data)
    return ''.join(rc)

def addFiles(arg, dirname, names):
	for name in names:
		path = os.path.join( dirname, name )
		if not os.path.samefile( path, arg[0] ):
			if not os.path.isdir(path):
				arg[1].append( os.path.normpath(path) )

def parseRemus(path, server):
	dir = os.path.dirname( path )
	handle = open( path )
	dom = parseString( handle.read() )
	handle.close()	
	includeFiles = []
	pipelineName = dom.childNodes[0].getAttribute("id")
	for node in dom.childNodes[0].childNodes:
		if node.nodeType == node.ELEMENT_NODE:
			if node.localName.startswith( "remus_" ):
				remusNode = {}
				remusNode['id'] = node.getAttribute('id')
				remusNode['input'] = node.getAttribute('input')
				remusNode['type'] = node.getAttribute('type' )
				remusNode['code'] = getText( node.childNodes )
				print server + "/" + pipelineName + ":" + remusNode['id'] 
				print json.dumps( remusNode )
			if node.localName == "include":
				includePath = os.path.join( dir, node.getAttribute("path") )
				if os.path.isdir( includePath ):
					os.path.walk( includePath, addFiles, [ path, includeFiles ] )
				else:
					includeFiles.append( includePath )
	for file in includeFiles:
		print server + "/" + pipelineName + "@attach/" + file.replace('/', '%2F')
if __name__ == "__main__":
	parseRemus( sys.argv[1], sys.argv[2] )
	
