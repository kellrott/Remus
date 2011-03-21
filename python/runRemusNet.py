#!/usr/bin/env python

import remus
import sys
import json
from urllib  import quote
import imp
from cStringIO import StringIO
import uuid
from urlparse import urlparse
import httplib
import tempfile
import traceback
import os
import shutil
import threading
import copy
import time

from remusLib import *

workerID = str(uuid.uuid4())

host = None
curServer = None
curConn = None
statusTimer = None

def urlopen(url,data=None,retry=1):
	u = urlparse( url )
	global curConn
	global curServer
	if curConn is None or curServer != u.netloc:
		curConn = httplib.HTTPConnection(u.netloc)
		curServer = u.netloc
	try:
		if data is not None:
			headers = {"Cookie":  'remusWorker=%s' % (workerID) }
			curConn.request("POST", u.path, data, headers)
			return StringIO( curConn.getresponse().read() )
		else:
			headers = {"Cookie":  'remusWorker=%s' % (workerID) }
			curConn.request("GET", u.path, None, headers)
			return StringIO( curConn.getresponse().read() )
	except httplib.BadStatusLine:
		if retry > 0:
			curConn = None
			urlopen( url, data, retry-1)

def statusPulse():
	global host
	global statusTimer
	log( "STATUS PULSE: " + workerID )
	
	u = urlparse( host )
	conn = httplib.HTTPConnection(u.netloc)
	headers = {"Cookie":  'remusWorker=%s' % (workerID) }
	conn.request("GET", "/@status", None, headers)
	conn.close()
	statusTimer = threading.Timer(60, statusPulse)
	statusTimer.start()
	

class http_write:
	def __init__(self, url, jobID):
		self.url = url
		self.order = 0
		self.jobID = jobID
		self.cache = []
		self.cacheMax = 10000
		
	def emit( self, key, value ):
		self.cache.append( [ copy.deepcopy(key), copy.deepcopy(value)] )
		if ( len(self.cache) > self.cacheMax ):
			self.flush()
	
	def close(self):
		self.flush()
		
	def flush(self):
		log("posting results: " + self.url)
		data = ""
		for out in self.cache:
			line = json.dumps( { 'id' : self.jobID, 'order' : self.order, 'key' : out[0] , 'value' : out[1] }  ) + "\n"
			data += line
			self.order += 1
			
		if (len(data)):
			urlopen(  self.url , data ).read()
		self.cache = []
	

getCache={}

def httpGetJson( url, useCache=False ):
	log( "getting: " + url )
	if ( useCache ):
		if not getCache.has_key( url ):
			getCache[ url ] = urlopen( url ).read()			
		handle = StringIO(getCache[ url ])
	else:
		handle = urlopen( url )
	return jsonIter( handle )


workerList = {}


def doWork( host, applet, instance, workDesc ): 
	worker = getWorker( host, applet )
	if worker is not None:
		worker.doWork(instance, workDesc)
	
if __name__=="__main__":
	host = sys.argv[1]
	remus.init(host)
	tmpDir = tempfile.mkdtemp()
	log( "TMPDIR: " + tmpDir )
	os.chdir( tmpDir )
	sys.path.append( tmpDir )
	if ( len(sys.argv) >= 3 ):
		workerID = sys.argv[2]
	statusPulse()
	try:
		retryCount = 6
		while retryCount > 0:
			workList = httpGetJson( host + "/@work?max=100" ).read()	
			if len(workList) == 0:
				retryCount -= 1
				time.sleep(10)
			else: 
				retryCount = 6
				for instance in workList:
					for node in workList[instance]:
						for workDesc in workList[instance][node]:
							doWork( host, node, instance, workDesc )
		shutil.rmtree( tmpDir ) 
	except:
		statusTimer.cancel()
		raise
	statusTimer.cancel()
