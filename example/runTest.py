#!/usr/bin/env python


import sys
from subprocess import call
from urlparse import urlparse
from urllib import urlopen
import httplib

server = "http://localhost:16017/"
host=urlparse( server )

conn = httplib.HTTPConnection(host.netloc)
conn.request( "DELETE", "/testPipeline@pipeline" )
print conn.getresponse().read()
conn.close()

call( "../bin/loadPipeline %s testPipeline.xml" % (server), shell=True )

submitData = """{ "testSubmission" : { "_applets" : ["testSplit"]  } }"""

print urlopen( "%s/testPipeline@submit" % (server), submitData ).read()

call( "../python/runRemusNet.py %s test1" % (server), shell=True )


