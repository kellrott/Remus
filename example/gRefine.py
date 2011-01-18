#!/usr/bin/env python

import json
import urllib
import urllib2

class Refine:
	def __init__(self, url):
		self.url = url
		
	def projectList(self):
		rUrl = self.url + "/command/core/get-all-project-metadata"
		data = urllib.urlopen( rUrl ).read()
		return (json.loads( data ))[ 'projects' ]
	
	def getTSV(self, projectID):
		rUrl = self.url + "/command/core/export-rows"
		data = "format=tsv&project=%s" % (projectID)
		return urllib.urlopen( rUrl, data )
	

if __name__ == "__main__":
	ref = Refine( "http://localhost:3333" )
	projects = ref.projectList()
	for projID in projects:
		print ref.getTSV( projID ).read()
