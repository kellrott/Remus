package org.remus.gossip;

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
import org.remus.thrift.BadPeerName;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.RemusGossip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerServer {

	public class PeerPingThread extends Thread {
		PeerServer parent;
		Boolean quit = false;

		PeerPingThread(PeerServer parent) {
			this.parent = parent;
			quit = false;
		}

		@Override
		public void run() {
			while (!quit) {
				doPing();
				try {
					synchronized (quit) {
						if (!quit) { 
							quit.wait(PING_TIME * 1000);
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();		
				}
			}
		}

		public void close() {
			quit = true;
		}

		public void reqPing() {
			synchronized (quit) {
				quit.notify();
			}
		}

		private synchronized void doPing() {
			try {
				parent.doPing();
			} catch (TException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * The default gossip port.
	 */
	public static final int GOSSIP_PORT = 15673;

	/**
	 * Default rate of pings.
	 */
	public static final int PING_TIME = 30;

	Peer localCopy;
	PeerInfoThrift localInfo;
	TServerSocket serverTransport;
	TServer server;

	private Logger logger;

	private PeerPingThread pingThread;

	public PeerServer(PeerInfoThrift info, String host, Integer hostPort) throws TException, BadPeerName, UnknownHostException, SocketException {
		init(info, host, hostPort, GOSSIP_PORT);
	}

	public PeerServer(PeerInfoThrift info, String host, Integer hostPort, int localPort) throws TException, BadPeerName, UnknownHostException, SocketException {
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


	private void init(PeerInfoThrift info, String host, Integer hostPort, int localPort) throws TException, BadPeerName, UnknownHostException, SocketException {
		logger = LoggerFactory.getLogger(PeerServer.class);
		serverTransport = new TServerSocket(localPort);
		localInfo = info;
		info.address = getDefaultAddress();
		info.port = localPort;
		pingThread = new PeerPingThread(this);
		localCopy = new Peer(info, pingThread );
		pingThread.start();
		logger.info(info.name + " Starting gossip on " + info.address + ":" + info.port);

		if (host != null && hostPort != null) {
			getPeers(host, hostPort);
			doPing();
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
				logger.info(localInfo.name + " Server Done");
			}			
		};
		serverThread.start();
	}

	public void close() throws TException {
		logger.info("Stopping gossip on " + localInfo.address + ":" + localInfo.port);
		PeerInfoThrift next = localCopy.getNext(localInfo.name);
		if (next != null) {
			logger.info(localInfo.name + " Telling Peer about STOP: " + next.name );
			try {
				TSocket transport = new TSocket(next.address, next.port);
				TBinaryProtocol protocol = new TBinaryProtocol(transport);
				RemusGossip.Client client = new RemusGossip.Client(protocol);	
				transport.open();
				client.delPeer(localInfo.name);
				transport.close();		
			} catch (TException e) {

			}
		}
		pingThread.close();
		server.stop();		
	}

	public List<PeerInfoThrift> getPeers() throws TException {
		return localCopy.getPeers();
	}

	public void getPeers(String server, int port) throws TException, BadPeerName {
		logger.info(localInfo.name + " Getting peers from " + server + ":" + port);
		TSocket transport = new TSocket(server, port);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		transport.open();
		RemusGossip.Client client = new RemusGossip.Client(protocol);
		for (PeerInfoThrift peer : client.getPeers()) {
			localCopy.addPeer(peer);
		}
		transport.close();
	}

	public void doPing() throws TException {
		PeerInfoThrift next = localCopy.getNext(localInfo.name);
		if (next != null) {
			logger.info( localInfo.name + " pinging " + next.name + " with " + localCopy.getPeers().size() + " peers" );
			TSocket transport = new TSocket(next.address, next.port);
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			RemusGossip.Client client = new RemusGossip.Client(protocol);
			transport.open();
			client.ping( localCopy.getPeers() );
			transport.close();
		} else {
			logger.info( localInfo.name + " is alone");
		}
	}


}
