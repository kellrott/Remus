package org.remus;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.thrift.TException;
import org.remus.plugin.PluginInterface;
import org.remus.plugin.PluginManager;
import org.remus.thrift.PeerType;
import org.remus.thrift.RemusNet;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemusIDMain extends RemusIDServer {

	private HashMap<String, PeerInfoThrift> peerMap;
	private HashMap<String, Long> lastPing;
	private PluginManager plugins;
	private Logger logger;


	@Override
	public void addPeer(PeerInfoThrift info) throws BadPeerName, TException, NotImplemented {
		synchronized (peerMap) {
			synchronized (lastPing) {
				if (info.name == null) {
					throw new BadPeerName();
				}
				logger.info("Adding peer type " + info.peerType + ": " + info.peerID + " " 
						+ info.name + " (" + info.host + ":" + info.port + ")");
				peerMap.put(info.peerID, info);
				lastPing.put(info.peerID, (new Date()).getTime());
			}			
		}	
	}


	@Override
	public void delPeer(String peerName) throws TException, NotImplemented {
		synchronized (peerMap) {
			logger.info("Removing Peer: " + peerName);
			peerMap.remove(peerName);
		}	
	}


	@Override
	public List<PeerInfoThrift> getPeers() throws TException, NotImplemented {
		List<PeerInfoThrift> out;
		synchronized (peerMap) {
			out = new LinkedList<PeerInfoThrift>();
			for (PeerInfoThrift pi : peerMap.values()){
				if (pi != null) {
					out.add(pi);
				}
			}
		}
		return out;
	}


	class PingThread extends Thread {
		private static final long SLEEPTIME = 30000;
		boolean quit = false;
		Integer waitLock = new Integer(0);

		@Override
		public void run() {
			while (!quit) {
				Set<String> peerList;
				synchronized (peerMap) {
					peerList = peerMap.keySet();
				}

				List<String> removeList = new ArrayList<String>();
				for (String peerID : peerList) {
					try {
						RemusNet.Iface iface = plugins.getPeer(peerID);
						iface.status();
					} catch (TException e) {
						//upon failure, remove node from peer list
						removeList.add(peerID);
					}
				}

				synchronized (peerMap) {
					for (String peerID : removeList) {					
						logger.error("PeerFailure peer: " + peerID);
						peerMap.remove(peerID);
					}
				}
				synchronized (waitLock) {			
					try {
						waitLock.wait(SLEEPTIME);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

		public void quit() {
			quit = true;
			synchronized (waitLock) {
				waitLock.notifyAll();	
			}
		}
	}

	/*
	public void ping(List<PeerInfoThrift> workers) throws TException {
		logger.info( local.name + " PINGED with " + workers.size() + " records" );
		boolean added = false;
		synchronized (peerMap) {
			synchronized (lastPing) {
				for (PeerInfoThrift worker : workers) {
					if (!peerMap.containsKey(worker.name)) {
						peerMap.put(worker.name, worker);
						added = true;
					}
					if (peerMap.get(worker.name) != null) {
						lastPing.put(worker.name, (new Date()).getTime());
					}
				}
			}
		}
		if (added) {
			logger.info( local.name + " learned about new peers" );
			callback.reqPing();
		}
	}
	 */


	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.name = "Main ID Server";
		out.peerType = PeerType.NAME_SERVER;
		return out;
	}


	@Override
	public void init(Map params) throws Exception {
		logger = LoggerFactory.getLogger(RemusIDMain.class);		
	}

	@Override
	public void preStart(PluginManager pm) throws Exception {
		peerMap = new HashMap<String, PeerInfoThrift>();
		lastPing = new HashMap<String, Long>();
		plugins = pm;		
		for (PluginInterface pi : pm.getPlugins()) {
			PeerInfo info = pi.getPeerInfo();
			info.setPeerID(UUID.randomUUID().toString());
			info.setHost(getDefaultAddress());
			info.setPort(pm.addLocalPeer(info.peerID, (RemusNet.Iface) pi));
			logger.info("Local Peer:" + info.name + " " + info.host + " " + info.port);
			addPeer(info);
		}
	}

	PingThread pingThread;
	@Override
	public void start(PluginManager pluginManager) throws Exception {
		pingThread = new PingThread();
		pingThread.start();
	}

	public void flushOld(long oldest) {
		long minPing = (new Date()).getTime() - oldest;
		List<String> removeList = new LinkedList<String>();
		synchronized (peerMap) {
			synchronized (lastPing) {
				for (String name : lastPing.keySet()) {
					if (lastPing.get(name) < minPing) {
						removeList.add(name);
					}
				}
				for (String name : removeList) {
					peerMap.put(name, null);
				}
			}
		}
	}


	@Override
	public void stop() {
		pingThread.quit();
	}


	@Override
	public String status() throws TException {
		return "OK";
	}
}
