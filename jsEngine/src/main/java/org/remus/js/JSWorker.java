package org.remus.js;

import java.util.Arrays;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.RemusWorker;
import org.remus.mapred.WorkEngine;
import org.remus.plugin.PluginManager;
import org.remus.thrift.JobStatus;
import org.remus.thrift.PeerType;
import org.remus.thrift.WorkDesc;
import org.remus.PeerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSWorker extends RemusWorker {

	PluginManager plugins;
	private Logger logger;
	
	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.peerType = PeerType.WORKER;
		out.name = "Rhino JavaScript";
		out.workTypes =  Arrays.asList("javascript");
		return out;
	}

	@Override
	public void init(Map params) {
		logger = LoggerFactory.getLogger(JSWorker.class);	

	}

	@Override
	public String jobRequest(String dataServer, WorkDesc work)
			throws TException {
		logger.info("Received job request: " + work.mode + " " + work.workStack);
		RemusDB db = plugins.getDataServer();		
		
		JSFunctionCall js = new JSFunctionCall();
		WorkEngine we = new WorkEngine(work, db, js);
		
		we.start();
		
		return "job";
	}

	@Override
	public JobStatus jobStatus(String jobID)
			throws TException {
		return JobStatus.DONE;
	}

	@Override
	public void start(PluginManager pluginManager) {
		plugins = pluginManager;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}	

}
