#!/usr/bin/env python

import sys
import uuid

import threading
from remus.thrift import RemusNet
from remus.thrift.ttypes import *
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from thrift.server import TServer

def log(e):
	sys.stderr.write("INFO: %s\n" % (e))

class IDInterface:
	def __init__(self, host, port):
		self.host = host
		self.port = port
		self.transport = TSocket.TSocket(host, port)
		self.transport = TTransport.TBufferedTransport(self.transport)
		self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
		self.idServer = RemusNet.Client(self.protocol)

	def getPeers(self):
		self.transport.open()
		self.peers = self.idServer.getPeers()
		self.transport.close()
		return self.peers

	def addSelf(self, port):
		self.localID = str(uuid.uuid1())
		p = PeerInfoThrift( peerType=PeerType.WORKER, 
			name="Python Worker",
			peerID=self.localID,
			workTypes=["python"],
			host="localhost", 
			port=port)
		
		log("Connecting as peer: %s" %(self.localID))
		self.transport.open()
		self.idServer.addPeer( p )
		self.transport.close()
	
	def close(self):
		self.transport.open()
		self.idServer.delPeer( self.localID )
		self.transport.close()

class RemusWorker(RemusNet.Iface):
	
	def __init__(self):
		pass
	
	def status(self):
		log("Status OK")
		return "OK"
		
class ServerThread(threading.Thread):
	def __init__(self, server):
		threading.Thread.__init__(self)
		self.server = server
	
	def run(self):
		self.server.serve()
		
class RemusWorkerServer:
	
	def __init__(self):
		self.processor = RemusNet.Processor(RemusWorker())
		
		self.socket = TSocket.TServerSocket()
		self.port = self.socket.port
		pfactory = TBinaryProtocol.TBinaryProtocolFactory()
		tfactory = TTransport.TBufferedTransportFactory()

		self.server = TServer.TThreadPoolServer( self.processor, self.socket, tfactory, pfactory)
		self.t = ServerThread(self.server)
		self.t.start()
	
	def stop(self):
		pass

if __name__ == "__main__":
	tmp = sys.argv[1].split(':')
	
	r = RemusWorkerServer()
	
	ns = IDInterface(tmp[0], tmp[1])	
	ns.addSelf(r.port)


	#print ns.getPeers()
	#ns.close()
	#print ns.getPeers()
	#r.stop()	
	
	#for p in ns.getPeers():
	#	if p.peerType == PeerType.MANAGER:
	#		print p
