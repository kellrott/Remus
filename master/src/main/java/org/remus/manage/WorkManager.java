package org.remus.manage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.remus.PeerInfo;
import org.remus.RemusManager;
import org.remus.core.RemusApp;
import org.remus.core.RemusPipeline;
import org.remus.core.WorkStatus;
import org.remus.plugin.PluginManager;
import org.remus.server.RemusDatabaseException;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author kellrott
 *
 */
public class WorkManager extends RemusManager {

	/***
	 * MANAGE_CONFIG = org.remus.workManage, conf string to define 
	 * which work managers to use.
	 */
	public static final String MANAGE_CONFIG = "org.remus.workManage";

	Logger logger;

	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.peerType = PeerType.MANAGER;
		return out;
	}

	@Override
	public void init(Map params) throws Exception {
		logger = LoggerFactory.getLogger(WorkManager.class);	

	}

	PluginManager plugins;
	@Override
	public void start(PluginManager pluginManager) throws Exception {
		plugins = pluginManager;
	}

	@Override
	public void scheduleRequest() throws TException, NotImplemented {

		Set<WorkStatus> fullList = new HashSet();
		try {
			RemusApp app = new RemusApp(plugins);
			for (String name : app.getPipelines()) {
				RemusPipeline pipe = app.getPipeline(name);
				Set<WorkStatus> curSet = pipe.getWorkQueue();
				fullList.addAll(curSet);
			}
			logger.info("MANAGER found " + fullList.size() + " active stacks");
		} catch (RemusDatabaseException e) {
			throw new TException(e);
		}
		
		
		
	}

	



}
