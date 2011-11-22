package org.remus.manage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.core.AppletInstance;
import org.remus.plugin.PeerManager;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;

public class WorkerPool {


	class WorkerInfo {
		PeerInfoThrift info;
		boolean inUse;
		InstanceWorker owner;

		WorkerInfo(PeerInfoThrift info) {
			this.info = info;
			inUse = false;
			owner = null;
		}

	}

	private PeerManager peerManager;
	private Map<String, WorkerInfo> peerInfo;

	public WorkerPool(PeerManager peerManager) {
		peerInfo = new HashMap<String, WorkerInfo>();
		this.peerManager = peerManager;
	}


	public void updateWorkers() throws NotImplemented, TException {
		//sync peer list
		synchronized (peerInfo) {
			Set<String> peers = peerManager.getWorkers();
			for (String peerID : peers) {
				if (!peerInfo.containsKey(peerID)) {
					peerInfo.put(peerID, new WorkerInfo(peerManager.getPeerInfo(peerID)));
				}
			}
			Set<String> removeSet = new HashSet<String>();
			for (String peer : peerInfo.keySet()) {
				if (!peers.contains(peer)) {
					removeSet.add(peer);
				}
			}
			for (String peer : removeSet) {
				peerInfo.remove(peer);
			}
		}
	}



	public int getKeyWorkerCount(String workType) {
		synchronized (peerInfo) {
			int count = 0;
			for (String peer : peerInfo.keySet()) {
				WorkerInfo pi = peerInfo.get(peer);
				if (!pi.inUse) {
					if (pi.info.workTypes.contains(workType)) {
						if (pi.info.isSetConfigJSON()) {
							Map config = (Map) JSON.loads(pi.info.configJSON);
							String mode = (String)config.get("_allocMode");
							if (mode == null || mode.compareTo("table") != 0){
								count++;
							}
						} else {
							count++;
						}
					}
				}
			}		
			return count;
		}
	}

	public int getTableWorkerCount(String workType) {
		synchronized (peerInfo) {			
			int count = 0;
			for (String peer : peerInfo.keySet()) {
				WorkerInfo pi = peerInfo.get(peer);
				if (!pi.inUse) {
					if (pi.info.workTypes.contains(workType)) {
						if (pi.info.isSetConfigJSON()) {
							Map config = (Map) JSON.loads(pi.info.configJSON);
							String mode = (String)config.get("_allocMode");
							if (mode != null && mode.compareTo("table") == 0){
								count++;
							}
						}
					}
				}
			}		
			return count;
		}
	}

	public InstanceWorker getInstanceWorker(AppletInstance ai) throws NotImplemented, TException {
		updateWorkers();
		if ( getTableWorkerCount(ai.getRecord().getType()) > 0 ) {
			InstanceWorker worker = new TableWorker(peerManager, this, ai);
			return worker;
		} 
		if ( getKeyWorkerCount(ai.getRecord().getType()) > 0) {
			InstanceWorker worker = new KeyWorker(peerManager, this, ai);
			return worker;
		}
		return null;
	}

	public void returnInstanceWorker(InstanceWorker instanceWorker) {
		synchronized (peerInfo) {
			for (String peer : peerInfo.keySet()) {
				WorkerInfo w = peerInfo.get(peer);
				if (w.owner == instanceWorker) {
					w.owner = null;
					w.inUse = false;
				}
			}
		}
	}

	public String borrowWorker(String type, InstanceWorker owner) {
		synchronized (peerInfo) {
			for (String peer : peerInfo.keySet()) {
				WorkerInfo w = peerInfo.get(peer);
				if (!w.inUse && w.info.workTypes.contains(type)) {
					w.inUse = true;
					w.owner = owner;
					return peer;
				}
			}
		}
		return null;
	}

	public void returnWorker(String peerID) {
		synchronized (peerInfo) {
			peerInfo.get(peerID).inUse = false;
			peerInfo.get(peerID).owner = null;
		}		
	}


	public void errorPeer(String peerID) {
		synchronized (peerInfo) {
			if (peerInfo.containsKey(peerID)) {
				peerInfo.get(peerID).inUse = false;
				peerInfo.get(peerID).owner = null;
			}
			peerManager.peerFailure(peerID);
		}
	}








}
