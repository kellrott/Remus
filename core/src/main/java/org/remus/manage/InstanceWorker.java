package org.remus.manage;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.json.simple.JSONAware;
import org.remus.RemusDatabaseException;
import org.remus.core.AppletInstance;
import org.remus.plugin.PeerManager;
import org.remus.thrift.NotImplemented;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InstanceWorker implements JSONAware {

	
	protected static final int WORKING = 0;
	protected static final int DONE = 1;
	protected static final int ERROR = 2;

	protected AppletInstance ai;
	protected Logger logger;
	protected Map<String,Boolean> peerList;
	protected PeerManager peerManager;
	protected WorkerPool workPool;
	
	int state;
	InstanceWorker(PeerManager peerManager, WorkerPool workPool, AppletInstance ai) {
		this.ai = ai;
		this.peerManager = peerManager;
		this.workPool = workPool;
		logger = LoggerFactory.getLogger(InstanceWorker.class);
		logger.debug("INSTANCE WORKER STARTING:" + ai);
	}
	
	
	public Set<String> getPeerSet() {
		return peerList.keySet();
	}
	

	abstract public boolean checkWork() throws NotImplemented, TException, InstanceWorkerException, RemusDatabaseException;
	abstract public boolean isDone();
	abstract public void removeJob();

}
