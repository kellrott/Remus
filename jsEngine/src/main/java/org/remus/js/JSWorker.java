package org.remus.js;

import java.util.Arrays;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.Worker;
import org.remus.mapred.WorkEngine;
import org.remus.plugin.PluginManager;
import org.remus.thrift.JobStatus;
import org.remus.thrift.PeerType;
import org.remus.thrift.WorkDesc;
import org.remus.PeerInfo;

public class JSWorker extends Worker {

	PluginManager plugins;
	
	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.peerType = PeerType.WORKER;
		out.workTypes =  Arrays.asList("javascript");
		return out;
	}

	@Override
	public void init(Map params) {
		
	}

	@Override
	public String jobRequest(String dataServer, WorkDesc work)
			throws TException {
		RemusDB db = plugins.getDataServer();
		JSFunctionCall js = new JSFunctionCall();
		WorkEngine we = new WorkEngine(work, db, js);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public JobStatus jobStatus(String jobID)
			throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void start(PluginManager pluginManager) {
		plugins = pluginManager;
	}	

}
