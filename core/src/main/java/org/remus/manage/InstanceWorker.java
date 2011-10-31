package org.remus.manage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.remus.core.AppletInstance;
import org.remus.plugin.PeerManager;
import org.remus.thrift.NotImplemented;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InstanceWorker {

	protected AppletInstance ai;
	protected Logger logger;
	protected Map<String,Boolean> peerList;
	protected PeerManager peerManager;
	
	InstanceWorker(PeerManager peerManager, AppletInstance ai) {
		this.ai = ai;
		this.peerManager = peerManager;
		logger = LoggerFactory.getLogger(InstanceWorker.class);
	}
	
	public void addPeer(String peerID) {
		if (peerList == null) {
			peerList = new HashMap<String,Boolean>();
		}
		peerList.put(peerID,false);
	}
	
	public Set<String> getPeerSet() {
		return peerList.keySet();
	}
	
	
	protected String borrowPeer() {
		for (String peer : peerList.keySet()) {
			if (!peerList.get(peer)) {
				peerList.put(peer, true);
				return peer;
			}
		}
		return null;
	}

	
	protected void returnPeer(String peer) {
		peerList.put(peer, false);
	}
	
	abstract public boolean checkWork() throws NotImplemented, TException;
	abstract public boolean isDone();
	abstract public void removeJob();

}
