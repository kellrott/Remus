#!/usr/bin/env python

import remus
import sys
import json
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

import remusLib

host = None
statusTimer = None

def statusPulse():
	global host
	global statusTimer
	remusLib.log( "STATUS PULSE: " + remusLib.workerID )
	
	u = urlparse( host )
	conn = httplib.HTTPConnection(u.netloc)
	headers = {"Cookie":  'remusWorker=%s' % (remusLib.workerID) }
	conn.request("GET", "/@status", None, headers)
	conn.close()
	statusTimer = threading.Timer(60, statusPulse)
	statusTimer.start()

	
workerList = {}


def doWork( host, pipeline, instance, applet, jobID, jobKey ): 
	worker = remusLib.getWorker( host, pipeline, instance, applet )
	if worker is not None:
		worker.doWork(jobID, jobKey)
	
if __name__=="__main__":
	host = sys.argv[1]

	tmpDir = tempfile.mkdtemp()
	remusLib.log( "TMPDIR: " + tmpDir )
	os.chdir( tmpDir )
	sys.path.append( tmpDir )
	if ( len(sys.argv) >= 3 ):
		remusLib.setWorkerID( sys.argv[2] )
	else:
		remusLib.setWorkerID( str(uuid.uuid4()) )

	statusPulse()
	try:
		retryCount = 3
		while retryCount > 0:
			workList = remusLib.httpGetJson( host + "/@work" ).read()	
			if len(workList) == 0:
				retryCount -= 1
				time.sleep(10)
			else: 
				retryCount = 3
				for pipeline in workList:
					for instance in workList[pipeline]:
						for applet in workList[pipeline][instance]:
							for jobID in workList[pipeline][instance][applet]:
								doWork( host, pipeline, instance, applet, jobID, [workList[pipeline][instance][applet][jobID]] )
		shutil.rmtree( tmpDir ) 
	except:
		statusTimer.cancel()
		raise
	statusTimer.cancel()
