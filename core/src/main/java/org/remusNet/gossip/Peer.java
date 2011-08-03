package org.remusNet.gossip;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remusNet.gossip.PeerServer.PeerPingThread;
import org.remusNet.thrift.BadPeerName;
import org.remusNet.thrift.PeerInfo;
import org.remusNet.thrift.RemusGossip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Peer implements RemusGossip.Iface {
	Map<String,PeerInfo> peerMap;
	Map<String,Long> lastPing;
	List<PeerInfo> addList;
	Logger logger;
	PeerInfo local;
	private PeerPingThread callback;
	
	public Peer(PeerInfo local, PeerServer.PeerPingThread callback ) throws TException, BadPeerName {
		logger = LoggerFactory.getLogger(Peer.class);
		peerMap = new HashMap<String, PeerInfo>();
		lastPing = new HashMap<String, Long>();
		addList = new LinkedList<PeerInfo>();
		this.local = local;
		this.callback = callback;
		addPeer(local);
	}

	@Override
	public void addPeer(PeerInfo info) throws TException, BadPeerName {
		synchronized (peerMap) {
			synchronized (lastPing) {
				if ( info.name == null ) {
					throw new BadPeerName();
				}
				logger.info(local.name + " Adding peer: " 
						+ info.name + " (" + info.address + ":" + info.port + ")");
				peerMap.put(info.name, info);
				lastPing.put(info.name, (new Date()).getTime());
			}			
		}
		callback.reqPing();
	}

	@Override
	public void delPeer(String peerName) throws TException {
		synchronized (peerMap) {
			peerMap.remove(peerName);
		}
	}

	@Override
	public List<PeerInfo> getPeers() throws TException {
		List<PeerInfo> out;
		synchronized (peerMap) {
			out = new LinkedList<PeerInfo>();
			for (PeerInfo pi : peerMap.values()){
				if (pi!=null) {
					out.add(pi);
				}
			}
		}
		return out;
	}

	@Override
	public void ping(List<PeerInfo> workers) throws TException {
		logger.info( local.name + " PINGED with " + workers.size() + " records" );
		boolean added = false;
		synchronized (peerMap) {
			synchronized (lastPing) {
				for (PeerInfo worker : workers) {
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

	/**
	 * First the next PeerInfo in the peer ring based on the 
	 * alphabetical order of names.
	 * @param name the name of the current node
	 * @return the next alphabetical hit from name
	 * @throws TException 
	 */
	public final PeerInfo getNext(final String name) throws TException {
		synchronized (peerMap) {
			List<String> peerNames = new LinkedList<String>();
			for (PeerInfo pi : peerMap.values()){
				if (pi!=null) {
					peerNames.add(pi.name);
				}
			}
			if (peerNames.size() == 0) {
				return null;
			}
			Collections.sort(peerNames);
			int i = peerNames.lastIndexOf(name);
			i = (i + 1) % (peerNames.size());
			if (peerNames.get(i).compareTo(name) == 0) {
				return null;
			}				
			return peerMap.get(peerNames.get(i));
		}
	}

}
