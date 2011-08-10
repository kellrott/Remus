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

	def addSelf(self):
		self.localID = str(uuid.uuid1())
		p = PeerInfoThrift( peerType=PeerType.WORKER, 
			name="Python Worker",
			peerID=self.localID,
			workTypes=["python"] )
		
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
		
class ServerThread(threading.Thread):
	def __init__(self, server):
		
class RemusWorkerServer:
	
	def __init__(self):
		self.processor = RemusNet.Processor(RemusWorker())
		
		self.socket = TSocket.TServerSocket()
		pfactory = TBinaryProtocol.TBinaryProtocolFactory()
		tfactory = TTransport.TBufferedTransportFactory()

		server = TServer.TThreadPoolServer( self.processor, self.socket, tfactory, pfactory)
		server.serve()		
		print self.socket

if __name__ == "__main__":
	tmp = sys.argv[1].split(':')
	
	"""
	ns = IDInterface(tmp[0], tmp[1])	
	print ns.getPeers()
	ns.addSelf()
	print ns.getPeers()
	ns.close()
	print ns.getPeers()
	"""
	
	r = RemusWorkerServer()
	
	#for p in ns.getPeers():
	#	if p.peerType == PeerType.MANAGER:
	#		print p
