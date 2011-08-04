package org.remus.manage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.PeerInfo;
import org.remus.RemusManager;
import org.remus.core.BaseNode;
import org.remus.core.RemusApp;
import org.remus.core.WorkAgent;
import org.remus.core.WorkStatus;
import org.remus.plugin.PluginManager;
import org.remus.server.RemusPipelineImpl;
import org.remus.thrift.PeerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author kellrott
 *
 */
public class WorkManager implements RemusManager {

	/***
	 * MANAGE_CONFIG = org.remus.workManage, conf string to define 
	 * which work managers to use.
	 */
	public static final String MANAGE_CONFIG = "org.remus.workManage";

	Logger logger;
	
	RemusApp app;

	private Map<WorkStatus, WorkAgent> workMap;


	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.peerType = PeerType.MANAGER;
		return out;
	}

	@Override
	public void init(Map params) throws Exception {
		// TODO Auto-generated method stub		
	}

	PluginManager plugins;
	@Override
	public void start(PluginManager pluginManager) throws Exception {
		plugins = pluginManager;
	}

	@Override
	public void scheduleRequest() throws TException {
		
		
		
		
	}

	



}
