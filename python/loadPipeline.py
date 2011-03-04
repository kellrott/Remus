#!/usr/bin/env python

import json
import sys
from xml.dom.minidom import parseString
import os
import httplib
from urlparse import urlparse
from cStringIO import StringIO

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

def putData( url, data ):
	u = urlparse( url )
	curConn = httplib.HTTPConnection(u.netloc)
	curServer = u.netloc
	curConn.request("PUT", u.path, data, {})
	outStr = StringIO( curConn.getresponse().read() )
	curConn.close()
	return outStr
	
def parseRemus(path, server):
	dir = os.path.dirname( path )
	handle = open( path )
	dom = parseString( handle.read() )
	handle.close()	
	includeFiles = []
	pipelineName = dom.childNodes[0].getAttribute("id")
	desc = dom.childNodes[0].getAttribute("desc")
	url = server + "/@pipeline/" + pipelineName 
	data = json.dumps( { "id" : pipelineName, "description" : desc } )
	print url
	print putData( url, data ).read()
	for node in dom.childNodes[0].childNodes:
		if node.nodeType == node.ELEMENT_NODE:
			if node.localName.startswith( "remus_" ):
				remusNode = {}
				remusNode['mode'] = node.localName.replace("remus_", "")
				remusNode['id'] = node.getAttribute('id')
				remusNode['input'] = node.getAttribute('input')
				remusNode['codeType'] = node.getAttribute('type' )
				remusNode['code'] = getText( node.childNodes )
				url = server + "/" + pipelineName + ":" + remusNode['id'] + "@pipeline"  
				print url
				data = json.dumps( remusNode )
				print putData( url, data ).read()
			if node.localName == "include":
				includePath = os.path.join( dir, node.getAttribute("path") )
				if os.path.isdir( includePath ):
					os.path.walk( includePath, addFiles, [ path, includeFiles ] )
				else:
					includeFiles.append( includePath )
	for file in includeFiles:
		url = server + "/" + pipelineName + "@attach/" + file.replace('/', '%2F')
		print url
		handle = open( file )
		print putData( url, handle.read() ).read()
		handle.close()
if __name__ == "__main__":
	parseRemus( sys.argv[1], sys.argv[2] )
	
