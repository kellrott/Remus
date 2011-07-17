#!/usr/bin/env python


import sys
import unittest

from subprocess import call
from urlparse import urlparse
from urllib import urlopen
import httplib



class MiniRunner( unittest.TestCase ):
	def setUp(self):
		sys.path.insert(0, '../python/')
		self.module = __import__( "runRemusMini" )


	def test_split(self):
		self.module.main( [ "pipeline_Basic.json", "testSplit", "input_Basic.json" ] )

		self.module.main( [ "pipeline_Basic.json", "testMap", "mini" ] )

		self.module.main( [ "pipeline_Basic.json", "testReduce", "mini" ] )


	def tearDown(self):		
		return
		

if __name__ == '__main__':
    unittest.main()
