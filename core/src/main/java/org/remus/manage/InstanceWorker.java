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

	
	protected static final int WORKING = 0;
	protected static final int DONE = 1;
	protected static final int ERROR = 2;

	protected AppletInstance ai;
	protected Logger logger;
	protected Map<String,Boolean> peerList;
	protected PeerManager peerManager;
	int state;
	InstanceWorker(PeerManager peerManager, AppletInstance ai) {
		this.ai = ai;
		this.peerManager = peerManager;
		logger = LoggerFactory.getLogger(InstanceWorker.class);
		logger.debug("INSTANCE WORKER STARTING:" + ai);
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
	
	protected void errorPeer(String peerID) throws InstanceWorkerException {
		peerList.remove(peerID);
		if (peerList.size() == 0) {
			throw new InstanceWorkerException("All workers failed");
		}
	}

	abstract public boolean checkWork() throws NotImplemented, TException, InstanceWorkerException;
	abstract public boolean isDone();
	abstract public void removeJob();

}
