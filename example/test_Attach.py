#!/usr/bin/env python


import sys
import unittest

from subprocess import call
from urlparse import urlparse
from urllib import urlopen
import httplib


server = "http://localhost:16017/"
host=urlparse( server )

class BasicPipeline( unittest.TestCase ):
	def setUp(self):
		conn = httplib.HTTPConnection(host.netloc)
		conn.request( "DELETE", "/@pipeline/testPipeline" )
		print conn.getresponse().read()
		conn.close()
		call( "../bin/loadPipeline %s pipeline_Attach.xml" % (server), shell=True )


	def test_submit(self):
		submitData = """{ "_applets" : ["testSplit"], "lineCount" : 15, "infoText" : "testing data"  }"""
		print urlopen( "%s/testPipeline/@submit/testSubmission" % (server), submitData ).read()
		call( "../python/runRemusNet.py %s test1" % (server), shell=True )


	def tearDown(self):		
		return
		conn = httplib.HTTPConnection(host.netloc)
		conn.request( "DELETE", "/@pipeline/testPipeline" )
		print conn.getresponse().read()
		conn.close()

if __name__ == '__main__':
    unittest.main()
