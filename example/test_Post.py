#!/usr/bin/env python


import sys
import unittest

from subprocess import call
from urlparse import urlparse
from urllib import urlopen
import httplib
import json

server = "http://localhost:16017/"
host=urlparse( server )

class BasicPipeline( unittest.TestCase ):
	def setUp(self):
		conn = httplib.HTTPConnection(host.netloc)
		conn.request( "DELETE", "/@pipeline/testPipeline" )
		print conn.getresponse().read()
		conn.close()
		call( "../bin/loadPipeline %s pipeline_Post.xml" % (server), shell=True )


	def test_submit(self):
		
		
		
		
	def tearDown(self):
		return
		conn = httplib.HTTPConnection(host.netloc)
		conn.request( "DELETE", "/@pipeline/testPipeline" )
		print conn.getresponse().read()
		conn.close()


if __name__ == '__main__':
    unittest.main()
