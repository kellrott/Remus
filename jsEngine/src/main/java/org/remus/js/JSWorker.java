package org.remus.js;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.thrift.TException;
import org.remus.RemusAttach;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class JSWorker extends RemusWorker {

	PluginManager plugins;
	private Logger logger;
	private static final int NTHREDS = 10;
	ExecutorService executor;
	
	Map<String,WorkEngine> workMap;
	
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
		executor = Executors.newFixedThreadPool(NTHREDS);
		workMap = new HashMap<String, WorkEngine>();
	}

	@Override
	public String jobRequest(String dataServer, WorkDesc work)
			throws TException {
		logger.info("Received job request: " + work.mode + " " + work.workStack);
		RemusDB db = plugins.getDataServer();
		RemusAttach attach = plugins.getAttachStore();
		
		JSFunctionCall js = new JSFunctionCall();
		WorkEngine we = new WorkEngine(work, db, attach, js);		
		executor.submit(we);
		String jobName = UUID.randomUUID().toString();
		workMap.put(jobName, we);
		return jobName;
	}

	@Override
	public JobStatus jobStatus(String jobID)
			throws TException {
		WorkEngine a = workMap.get(jobID);
		if (a != null) {
			return a.getStatus();
		}			
		return JobStatus.UNKNOWN;
	}

	@Override
	public void start(PluginManager pluginManager) {
		plugins = pluginManager;
	}

	@Override
	public void stop() {
		executor.shutdown();
		// Wait until all threads are finish
		while (!executor.isTerminated()) {

		}		
	}	

}
