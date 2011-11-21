#!/usr/bin/env python


import sys
import unittest
import csv

import json

from subprocess import call
from urlparse import urlparse
from urllib import urlopen
import httplib


server = "http://localhost:16016/"
host=urlparse( server )


dataName = "dataTable"

dataDesc = {
	"_mode" : "store"
}

appletName = "maxTable"

appletDesc = {
	"_mode" : "map",
	"_type" : "javascript",
	"_auto" : False,
	"_src"  : "geneTable",
	"_script" : """
function(x,y) {
	sum = 0.0;
	for (var i in y) {
		sum += parseFloat(y[i]);
	}
	remus.emit(x, {"sum" : sum});
}
	"""
}

dataSubmitName = "database"

dataSubmit = {
}

appletSubmitName = "appletbase"

appletSubmit = {
	"_submitInit" : [appletName],
	"_submitInput" : { appletName : { "geneTable" : { "_applet" : dataName, "_instance" : dataSubmitName } } }
}


class BasicPipeline( unittest.TestCase ):
	def setUp(self):
		conn = httplib.HTTPConnection(host.netloc)
		conn.request( "DELETE", "/@pipeline/testPipeline" )
		print conn.getresponse().read()
		
		conn.request("PUT", "/@pipeline/testPipeline", json.dumps({}), {})
		print conn.getresponse().read()
		
		conn.request("PUT", "/testPipeline/@pipeline/%s" % (dataName), json.dumps(dataDesc), {})
		print conn.getresponse().read()
		
		conn.request("PUT", "/testPipeline/@pipeline/%s" % (appletName), json.dumps(appletDesc), {})
		print conn.getresponse().read()
		
		
		conn.request("POST", "/testPipeline/@submit/%s" % (dataSubmitName), json.dumps(dataSubmit), {})
		print conn.getresponse().read()
		
		handle = open("test.matrix")
		reader = csv.reader(handle, delimiter="\t")
		row_head = None
		for row in reader:
			if row_head is None:
				row_head = {}
				for i in range(len(row)):
					row_head[i] = row[i]
			else:
				name = row[0]
				data = {}
				for i in range(1,len(row)):
					data[ row_head[i] ]= row[i]
				conn.request("PUT", "/testPipeline/%s/%s/%s" % (dataSubmitName, dataName, name), json.dumps(data), {})
				print conn.getresponse().read()
		handle.close()
		conn.close()
		
	def test_submit(self):
	
		conn = httplib.HTTPConnection(host.netloc)

		conn.request("POST", "/testPipeline/@submit/%s" % (appletSubmitName), json.dumps(appletSubmit), {})
		print conn.getresponse().read()

		

	
		pass


	def tearDown(self):		
		return 
		conn = httplib.HTTPConnection(host.netloc)
		conn.request( "DELETE", "/@pipeline/testPipeline" )
		print conn.getresponse().read()
		conn.close()

if __name__ == '__main__':
    unittest.main()
