package org.remus.manage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.PeerInfo;
import org.remus.core.AppletInstance;
import org.remus.plugin.PeerManager;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;

public class WorkerPool {


	class WorkerInfo {
		PeerInfoThrift info;
		boolean inUse;

		WorkerInfo(PeerInfoThrift info) {
			this.info = info;
			inUse = false;
		}

	}

	private PeerManager peerManager;
	private Map<String, WorkerInfo> peerInfo;

	public WorkerPool(PeerManager peerManager) {
		peerInfo = new HashMap<String, WorkerInfo>();
		this.peerManager = peerManager;
	}


	public InstanceWorker getWorker(AppletInstance ai) throws NotImplemented, TException {

		//sync peer list
		Set<String> peers = peerManager.getWorkers();
		for (String peerID : peers) {
			if (!peerInfo.containsKey(peerID)) {
				peerInfo.put(peerID, new WorkerInfo(peerManager.getPeerInfo(peerID)));
			}
		}

		List<String> peerCollection = new LinkedList<String>();
		String peerMaster = null;
		for (String peer : peerInfo.keySet()) {
			WorkerInfo pi = peerInfo.get(peer);
			if (!pi.inUse) {
				if (pi.info.workTypes.contains(ai.getApplet().getType())) {
					if (pi.info.isSetConfigJSON()) {
						Map config = (Map) JSON.loads(pi.info.configJSON);
						String mode = (String)config.get("_allocMode");
						if (mode != null && mode.compareTo("table") == 0){
							peerMaster = null;
						}
					} else {
						peerCollection.add(peer);
					}
				}
			}
		}
		if (peerMaster != null ) {

		} 
		if (peerCollection.size() > 0) {
			InstanceWorker worker = new KeyWorker(peerManager, ai);
			for (String peer : peerCollection) {
				peerInfo.get(peer).inUse = true;
				worker.addPeer(peer);
			}
			return worker;
		}
		return null;
	}



	public void returnWorker(InstanceWorker iw) {
		for (String peerID : iw.getPeerSet()) {
			peerInfo.get(peerID).inUse = false;
		}
	}


}
