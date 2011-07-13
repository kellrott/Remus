package org.remus.manage;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.RemusApp;
import org.remus.RemusPipeline;
import org.remus.serverNodes.BaseNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author kellrott
 *
 */
public class WorkManager implements BaseNode {

	/***
	 * MANAGE_CONFIG = org.remus.workManage, conf string to define 
	 * which work managers to use.
	 */
	public static final String MANAGE_CONFIG = "org.remus.workManage";

	Logger logger;
	RemusApp app;
	WorkAgent agent;
	@SuppressWarnings("rawtypes")
	public WorkManager(RemusApp app ) {
		logger = LoggerFactory.getLogger(WorkManager.class);

		this.app = app;
		Map params = app.getParams();
		String managerName = (String)params.get(MANAGE_CONFIG);		
		try {
			Class<?> agentClass = Class.forName(managerName);			
			agent = (WorkAgent) agentClass.newInstance();
			agent.init(this);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		workMap = new HashMap<WorkStatus, WorkAgent>();
		jobScan();
		workPoll();
	}

	@Override
	public BaseNode getChild(final String name) {
		return agent.getChild(name);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		agent.doGet(name, params, workerID, serial, os);
	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		agent.doPut(name, workerID, serial, is, os);
	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		agent.doSubmit(name, workerID, serial, is, os);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void doDelete(String name, Map params, String workerID)
			throws FileNotFoundException {
		agent.doDelete(name, params, workerID);
	}


	private Map<WorkStatus, WorkAgent> workMap;

	public WorkStatus requestWorkStack(WorkAgent agent, Collection<String> codeTypes) {
		synchronized (workMap) {
			WorkStatus out = null;
			do {
				for (WorkStatus a : workMap.keySet()) {
					if (workMap.get(a) == null && codeTypes.contains(a.getApplet().getType())) {
						out = a;
					}
				}
				
				if ( out == null ) {
					if ( !jobScan() ) {
						logger.info("Agent " + agent.getName() + " found empty active stacks");
						return null;
					}
				}
			} while ( out == null );
			if ( out != null ) {
				logger.info("Agent " + agent.getName() + " checkout " + out.toString());
				workMap.put(out, agent);
			}
			return out;
		}		
	}

	public void returnWorkStack( WorkStatus stack ) {
		synchronized ( workMap ) {
			logger.info("Agent " + workMap.get(stack).getName() + " returning " + stack);
			workMap.put(stack, null);
		}		
	}

	public void completeWorkStack( WorkStatus stack ) {
		synchronized ( workMap ) {
			logger.info( "Agent " + workMap.get(stack).getName() + " completed " + stack );
			workMap.remove(stack);
		}
	}

	public boolean jobScan() {
		boolean newWork = false;
		logger.info("Starting jobScan");
		for ( RemusPipeline pipeline : app.getPipelines() ) {			
			for ( WorkStatus ws : pipeline.getWorkQueue( ) ) {
				if ( !workMap.containsKey(ws) ) {
					workMap.put(ws, null);
					newWork = true;
				}
			}
		}
		logger.info("jobScan done " + workMap.size() + " active appletInstances");
		return newWork;
	}

	public void workPoll() {
		agent.workPoll();
	}


}
