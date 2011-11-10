package org.remus.plugin;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import org.remus.thrift.BadPeerName;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerAddress;
import org.remus.thrift.RemusNet;
import org.remus.thrift.RemusNet.Processor;
import org.slf4j.LoggerFactory;


public class PluginManager {

	public static int START_PORT=16020;

	List<PluginInterface> plugins;
	Map<PluginInterface, ServerThread> servers;
	private org.slf4j.Logger logger;
	private PeerManager peerManager;

	private class ServerThread extends Thread {
		public ServerThread(int port, RemusNet.Iface plug) throws TTransportException {
			//try {
			this.port = port;

			TServerSocket serverTransport = new TServerSocket(port);
			Processor processor = new RemusNet.Processor((RemusNet.Iface) plug);
			Factory protFactory = new TBinaryProtocol.Factory(true, true);
			TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
			args.inputProtocolFactory(protFactory);
			args.processor(processor);
			server = new TThreadPoolServer(args);		
			//} catch (TException e) {
			//	e.printStackTrace();
			//}
		}
		TServer server;
		int port;
		@Override
		public void run() {
			server.serve();
		}
	}

	public PluginManager(Map<String, Object> params) throws NotImplemented, BadPeerName, TException {		
		logger = LoggerFactory.getLogger(PluginManager.class);

		List<String> seeds = (List)params.get("seeds");

		if (seeds == null) {
			logger.warn("NO SEEDS LISTED");
			peerManager = new PeerManager(this, new LinkedList<PeerAddress>());
		} else {
			List<PeerAddress> sList = new LinkedList<PeerAddress>();
			for (String sStr : seeds) {
				String []tmp = sStr.split(":");
				PeerAddress addr = new PeerAddress();
				addr.setHost(tmp[0]);
				if (tmp.length > 1) {
					addr.setPort(Integer.parseInt(tmp[1]));
				} else {
					addr.setPort(START_PORT);
				}
				sList.add(addr);
			}
			peerManager = new PeerManager(this, sList);
		}

		int defaultPort = START_PORT;

		plugins = new LinkedList<PluginInterface>();
		servers = new HashMap<PluginInterface, ServerThread>();

		for (String className : params.keySet()) {
			if (className.compareTo("config") == 0) {
				Map pMap = (Map) params.get(className);
				if (pMap.containsKey("log4jParamFile")) {
					PropertyConfigurator.configure((String)pMap.get("log4jParamFile"));
				}
			} 
		}

		for (String className : params.keySet()) {
			if (className.compareTo("config") == 0) {

			} else if (className.compareTo("seeds") == 0) {

			} else {
				try {
					Map pMap = (Map) params.get(className);
					Class<PluginInterface> pClass = 
							(Class<PluginInterface>) Class.forName(className);

					Map config = null;
					Map serverConf = null;
					if (pMap != null) {
						config = (Map) pMap.get("config");
						serverConf = (Map) pMap.get("server");
					}
					PluginInterface plug = (PluginInterface) pClass.newInstance();
					plug.init(config);
					plugins.add(plug);

					int port;
					if (serverConf != null) {
						port = Integer.parseInt(serverConf.get("port").toString());
					} else {
						port = defaultPort;
						defaultPort++;
					}

					int retryCount = 25;
					do { 
						try {
							ServerThread sThread = new ServerThread(port, (RemusNet.Iface) plug);
							sThread.start();
							servers.put(plug, sThread);
							plug.setupPeer(peerManager);
							String peerAddr = PeerManager.getDefaultAddress();
							String peerid = peerManager.addLocalPeer(plug, new PeerAddress(peerAddr, port));
							logger.info("Opening " + className + " server " + peerid + " at " +  peerAddr + " on port " + port);
							
							retryCount = 0;

						} catch (TTransportException e) {
							retryCount--;
							port+=1;
						}
					} while (retryCount > 0);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public void start() throws Exception {
		for (PluginInterface p : plugins) {
			p.start(this);
		}
	}


	public void close() {
		for (PluginInterface pi : plugins) {
			pi.stop();
		}
		for (ServerThread server : servers.values()){
			server.server.stop();
		}
		peerManager.stop();
	}


	public List<PluginInterface> getPlugins() {
		return plugins;
	}

	public PeerManager getPeerManager() {
		return peerManager;
	}

}
