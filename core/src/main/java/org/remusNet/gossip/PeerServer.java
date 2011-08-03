package org.remusNet.gossip;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.remusNet.thrift.BadPeerName;
import org.remusNet.thrift.PeerInfo;
import org.remusNet.thrift.RemusGossip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerServer {

	/**
	 * The default gossip port
	 */
	public static final int GOSSIP_PORT = 15673;

	/**
	 * Defaul rate of pings
	 */
	public static final int PING_TIME = 30;

	Peer localCopy;
	PeerInfo localInfo;
	TServerSocket serverTransport;
	TServer server;

	private Logger logger;

	public PeerServer(PeerInfo info, String host, Integer hostPort) throws TException, BadPeerName, UnknownHostException, SocketException {
		init(info, host, hostPort, GOSSIP_PORT);
	}

	public PeerServer(PeerInfo info, String host, Integer hostPort, int localPort) throws TException, BadPeerName, UnknownHostException, SocketException {
		init(info, host, hostPort, localPort);
	}

	public String getAddress() {
		return serverTransport.getServerSocket().toString();
	}

	public String getDefaultAddress() throws UnknownHostException, SocketException {

		return "127.0.0.1";

/*
		for (Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
			NetworkInterface ifc = ifaces.nextElement();
			if(ifc.isUp()) {
				for( Enumeration<InetAddress> addres = ifc.getInetAddresses(); addres.hasMoreElements(); ) {
					InetAddress addr = addres.nextElement();
					return addr.getHostAddress();
				}
			}
		}
		return null;
*/
	}


	private void init(PeerInfo info, String host, Integer hostPort, int localPort) throws TException, BadPeerName, UnknownHostException, SocketException {

		logger = LoggerFactory.getLogger(PeerServer.class);

		serverTransport = new TServerSocket(localPort);
		localInfo = info;
		info.address = getDefaultAddress();
		info.port = localPort;
		localCopy = new Peer(info);

		logger.info("Starting gossip on " + info.address + ":" + info.port);
		
		if (host != null && hostPort != null) {
			getPeers(host, hostPort);
		}

		RemusGossip.Processor processor = new RemusGossip.Processor(localCopy);
		Factory protFactory = new TBinaryProtocol.Factory(true, true);
		Args args = new TThreadPoolServer.Args(serverTransport);
		args.inputProtocolFactory(protFactory);
		args.processor(processor);
		server = new TThreadPoolServer(args);		 

		Thread serverThread = new Thread() {			
			@Override
			public void run() {
				server.serve();
			}			
		};
		serverThread.start();
	}

	public void close() throws TException {
		logger.info("Stopping gossip on " + localInfo.address + ":" + localInfo.port);
		PeerInfo next = localCopy.getNext(localInfo.name);
		if (next != null) {
			logger.info("Telling Peer about STOP: " + next.address + ":" + next.port);
			TSocket transport = new TSocket(next.address, next.port);
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			RemusGossip.Client client = new RemusGossip.Client(protocol);	
			transport.open();
			client.delPeer(localInfo.name);
			transport.close();
		}
		server.stop();		
	}

	public List<PeerInfo> getPeers() throws TException {
		return localCopy.getPeers();
	}

	public void getPeers(String server, int port) throws TException, BadPeerName {
		logger.info(localInfo.name + " Getting peers from " + server + ":" + port);
		TSocket transport = new TSocket(server, port);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		transport.open();
		RemusGossip.Client client = new RemusGossip.Client(protocol);
		for (PeerInfo peer : client.getPeers()) {
			localCopy.addPeer(peer);
		}
		transport.close();
	}
	
	public void doPing() throws TTransportException {
		PeerInfo next = localCopy.getNext(localInfo.name);
		if (next != null) {
		TSocket transport = new TSocket(next.address, next.port);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		RemusGossip.Client client = new RemusGossip.Client(protocol);
		transport.open();
		
		//client.ping( localCopy.getPeers() );
		
		transport.close();
		}
	}
	

	/*
	class PingThread extends Thread {
		TSocket transport;
		TBinaryProtocol protocol;
		RemusGossip.Client client;

		public PingThread() {
			transport = new TSocket("localhost", GOSSIP_PORT);
			protocol = new TBinaryProtocol(transport);
			client = new RemusGossip.Client(protocol);
		}

		@Override
		public void run() {
		client.pi
		}		
	}
	 */

}
