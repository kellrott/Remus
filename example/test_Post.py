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


datatest = {
	"root" : { "id" : 0, "home": "/root", "title":"root" },
	"daemon" : { "id" : 1, "home": "/usr/sbin", "title":"daemon" },
	"bin" : { "id" : 2, "home": "/bin", "title":"bin" },
	"sys" : { "id" : 3, "home": "/dev", "title":"sys" },
	"sync" : { "id" : 4, "home": "/bin", "title":"sync" },
	"games" : { "id" : 5, "home": "/usr/games", "title":"games" },
	"man" : { "id" : 6, "home": "/var/cache/man", "title":"man" },
	"lp" : { "id" : 7, "home": "/var/spool/lpd", "title":"lp" },
	"mail" : { "id" : 8, "home": "/var/mail", "title":"mail" },
	"news" : { "id" : 9, "home": "/var/spool/news", "title":"news" },
	"uucp" : { "id" : 10, "home": "/var/spool/uucp", "title":"uucp" },
	"proxy" : { "id" : 13, "home": "/bin", "title":"proxy" },
	"www-data" : { "id" : 33, "home": "/var/www", "title":"www-data" },
	"backup" : { "id" : 34, "home": "/var/backups", "title":"backup" },
	"list" : { "id" : 38, "home": "/var/list", "title":"Mailing List Manager" },
	"irc" : { "id" : 39, "home": "/var/run/ircd", "title":"ircd" },
	"gnats" : { "id" : 41, "home": "/var/lib/gnats", "title":"Gnats Bug-Reporting System (admin)" },
	"nobody" : { "id" : 65534, "home": "/nonexistent", "title":"nobody" },
	"libuuid" : { "id" : 100, "home": "/var/lib/libuuid", "title":"" },
	"syslog" : { "id" : 101, "home": "/home/syslog", "title":"" },
	"messagebus" : { "id" : 102, "home": "/var/run/dbus", "title":"" },
	"avahi-autoipd" : { "id" : 103, "home": "/var/lib/avahi-autoipd", "title":"Avahi autoip daemon,,," },
	"avahi" : { "id" : 104, "home": "/var/run/avahi-daemon", "title":"Avahi mDNS daemon,,," },
	"couchdb" : { "id" : 105, "home": "/var/lib/couchdb", "title":"CouchDB Administrator,,," },
	"usbmux" : { "id" : 106, "home": "/home/usbmux", "title":"usbmux daemon,,," },
	"speech-dispatcher" : { "id" : 107, "home": "/var/run/speech-dispatcher", "title":"Speech Dispatcher,,," },
	"kernoops" : { "id" : 108, "home": "/", "title":"Kernel Oops Tracking Daemon,,," },
	"pulse" : { "id" : 109, "home": "/var/run/pulse", "title":"PulseAudio daemon,,," },
	"rtkit" : { "id" : 110, "home": "/proc", "title":"RealtimeKit,,," },
	"saned" : { "id" : 111, "home": "/home/saned", "title":"" },
	"hplip" : { "id" : 112, "home": "/var/run/hplip", "title":"HPLIP system user,,," },
	"gdm" : { "id" : 113, "home": "/var/lib/gdm", "title":"Gnome Display Manager" },
	"sshd" : { "id" : 114, "home": "/var/run/sshd", "title":"" },
	"tomcat6" : { "id" : 115, "home": "/usr/share/tomcat6", "title":"" },
	"mysql" : { "id" : 116, "home": "/nonexistent", "title":"MySQL Server,,," },
}


class BasicPipeline( unittest.TestCase ):
	def setUp(self):
		conn = httplib.HTTPConnection(host.netloc)
		conn.request( "DELETE", "/@pipeline/testPipeline" )
		print conn.getresponse().read()
		conn.close()
		call( "../bin/loadPipeline %s pipeline_Post.xml" % (server), shell=True )


	def test_submit(self):
		submitData = """{ "info" : "metadata"  }"""
		print urlopen( "%s/testPipeline/@submit/dataTest" % (server), submitData ).read()
		#call( "../python/runRemusNet.py %s test1" % (server), shell=True )

		conn = httplib.HTTPConnection(host.netloc)
		for key in datatest:
			conn.request( "PUT", "/testPipeline/dataTest/dataStack/%s" % (key), json.dumps( datatest[key] ) )
			print conn.getresponse().read()
		conn.close()
		
		for key in datatest:
			data = json.loads( urlopen( "%s/testPipeline/dataTest/dataStack/%s" % (server, key) ).read() )
			for k in data:
				self.assertEqual( k, key )
				for e in datatest[key]:
					self.assertTrue( data[k].has_key( e ) )

		
	def tearDown(self):
		conn = httplib.HTTPConnection(host.netloc)
		conn.request( "DELETE", "/@pipeline/testPipeline" )
		print conn.getresponse().read()
		conn.close()


if __name__ == '__main__':
    unittest.main()
