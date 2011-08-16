package org.remus.plugin;

import java.net.ServerSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusIDServer;
import org.remus.RemusManager;
import org.remus.RemusRemote;
import org.remus.RemusWorker;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.PeerType;
import org.remus.thrift.RemusNet;
import org.remus.thrift.RemusNet.Processor;
import org.slf4j.LoggerFactory;


public class PluginManager {

	List<PluginInterface> plugins;
	Map<PluginInterface, ServerThread> servers;
	private org.slf4j.Logger logger;

	private class ServerThread extends Thread {
		public ServerThread(int port, RemusNet.Iface plug) {
			try {
				this.port = port;
				TServerSocket serverTransport = new TServerSocket(port);
				Processor processor = new RemusNet.Processor((RemusNet.Iface) plug);
				Factory protFactory = new TBinaryProtocol.Factory(true, true);
				TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
				args.inputProtocolFactory(protFactory);
				args.processor(processor);
				server = new TThreadPoolServer(args);		
			} catch (TException e) {
				e.printStackTrace();
			}
		}
		TServer server;
		int port;
		@Override
		public void run() {
			server.serve();
		}
	}

	public PluginManager(Map<String,Object> params) {
		logger = LoggerFactory.getLogger(PluginManager.class);

		peerList = new HashMap<String, RemusNet.Iface>();
		localPeers = new HashSet<String>();

		plugins = new LinkedList<PluginInterface>();
		servers = new HashMap<PluginInterface, ServerThread>();
		for (String className : params.keySet()) {
			try {
				Map pMap = (Map) params.get(className);
				if (pMap.containsKey("client")) {
					Class<PluginInterface> pClass = 
						(Class<PluginInterface>) Class.forName(className);
					PluginInterface plug = (PluginInterface) pClass.newInstance();
					Map config = (Map) pMap.get("client");
					plug.init(config);
					plugins.add(plug);
				} else if (pMap.containsKey("server")) {
					Map serverConf = (Map) pMap.get("server");
					Map config = (Map) pMap.get("config");
					Class<PluginInterface> pClass = 
						(Class<PluginInterface>) Class.forName(className);
					PluginInterface plug = (PluginInterface) pClass.newInstance();
					plug.init(config);
					plugins.add(plug);
					int port = Integer.parseInt(serverConf.get("port").toString());
					ServerThread sThread = new ServerThread(port, (RemusNet.Iface) plug);
					sThread.start();
					servers.put(plug, sThread);
				} else {
					Class<PluginInterface> pClass = 
						(Class<PluginInterface>) Class.forName(className);
					PluginInterface plug = (PluginInterface) pClass.newInstance();
					Map config = null;
					if (pMap.containsKey("config")) {
						config = (Map) pMap.get("config");
					}
					plug.init(config);
					plugins.add(plug);
				}
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

	public void start() throws Exception {
		for (PluginInterface p : plugins) {
			p.start(this);
		}
	}

	public RemusDB getDataServer() {
		for (PluginInterface pi : plugins) {
			if (pi.getPeerInfo().peerType == PeerType.DB_SERVER) {
				return (RemusDB) pi;
			}
		}
		return null;
	}

	public RemusAttach getAttachStore() {
		for (PluginInterface pi : plugins) {
			if (pi.getPeerInfo().peerType == PeerType.ATTACH_SERVER) {
				return (RemusAttach) pi;
			}
		}
		return null;
	}

	public void close() {
		for (PluginInterface pi : plugins) {
			pi.stop();
		}
		for (ServerThread server : servers.values()){
			server.server.stop();
		}
	}


	public RemusManager getManager() {
		for (PluginInterface pi : plugins) {
			if (pi.getPeerInfo().peerType == PeerType.MANAGER) {
				return (RemusManager) pi;
			}
		}
		return null;		
	}


	public RemusIDServer getIDServer() {
		for (PluginInterface pi : plugins) {
			if (pi.getPeerInfo().peerType == PeerType.NAME_SERVER) {
				return (RemusIDServer) pi;
			}
		}
		return null;		
	}



	/*
	public Set<RemusWorker> getWorkers() {
		Set<RemusWorker> out = new HashSet<RemusWorker>();
		for (PluginInterface pi : plugins) {
			if (pi.getPeerInfo().peerType == PeerType.WORKER) {
				out.add((RemusWorker) pi);
			}
		}
		return out;
	}
	 */

	Map<String,RemusNet.Iface> peerList;
	Set<String> localPeers;

	public int addLocalPeer(String peerID, RemusNet.Iface iface) {
		peerList.put(peerID, iface);
		localPeers.add(peerID);
		if (servers.containsKey(iface)) {
			return servers.get(iface).port;
		}
		return 0;
	}

	public Set<String> getWorkers(String type) throws NotImplemented, TException {
		Set<String> out = new HashSet<String>();
		List<PeerInfoThrift> list = getIDServer().getPeers();
		for (PeerInfoThrift p : list) {
			if (p.peerType == PeerType.WORKER) {
				if (p.workTypes.contains(type)) {
					out.add(p.peerID);
				}
			}
		}
		return out;
	}
	/*
	public void setRemotePeers(List<PeerInfoThrift> rPeers) {
		try {
			for (PeerInfoThrift pi : rPeers) {
				if (!peerList.containsKey(pi.peerID)) {
					logger.info("Remote Peer" + pi.name + " " + pi.peerID);
					peerList.put(pi.peerID, RemusRemote.getClient(pi.host, pi.port));
				}
			}
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	 */

	public RemusNet.Iface getPeer(String peerID) {
		if (peerList.containsKey(peerID)) {
			return peerList.get(peerID);
		}
		try {
			List<PeerInfoThrift> out = getIDServer().getPeers();
			for (PeerInfoThrift p : out) {
				if (p.peerID.compareTo(peerID) == 0) {
					return RemusRemote.getClient(p.host, p.port);
				}
			}
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public String getPeerID(RemusNet.Iface plug) {
		for (String key : peerList.keySet()) {
			if (peerList.get(key) == plug) {
				return key;
			}
		}
		return null;
	}

	public boolean isLocalPeer(String peerID) {
		if (localPeers.contains(peerID)) {
			return true;
		}
		return false;
	}

	public List<PluginInterface> getPlugins() {
		return plugins;
	}

}
