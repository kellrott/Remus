package org.remus;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.thrift.TException;
import org.remus.manage.Util;
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
				logger.info("Adding peer: " + info.peerID + " " 
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
	public void start(PluginManager pluginManager) throws Exception {
		peerMap = new HashMap<String, PeerInfoThrift>();
		lastPing = new HashMap<String, Long>();
		plugins = pluginManager;		
		for (PluginInterface pi : pluginManager.getPlugins()) {
			PeerInfo info = pi.getPeerInfo();
			info.setPeerID(UUID.randomUUID().toString());
			info.setHost(Util.getDefaultAddress());
			info.setPort(pluginManager.addLocalPeer(info.peerID, (RemusNet.Iface) pi));
			logger.info("Local Peer:" + info.name + " " + info.host + " " + info.port);
			addPeer(info);
		}		
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
		// TODO Auto-generated method stub
		
	}

}
