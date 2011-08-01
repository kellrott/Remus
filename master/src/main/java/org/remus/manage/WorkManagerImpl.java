package org.remus.manage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.remus.BaseNode;
import org.remus.WorkAgent;
import org.remus.WorkManager;
import org.remus.WorkStatus;
import org.remus.server.RemusApp;
import org.remus.server.RemusPipelineImpl;
import org.remusNet.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author kellrott
 *
 */
public class WorkManagerImpl implements BaseNode, WorkManager {

	/***
	 * MANAGE_CONFIG = org.remus.workManage, conf string to define 
	 * which work managers to use.
	 */
	public static final String MANAGE_CONFIG = "org.remus.workManage";

	Logger logger;
	RemusApp app;
	Map<String, WorkAgent> agentList;
	@SuppressWarnings("rawtypes")
	public WorkManagerImpl(RemusApp app ) {
		logger = LoggerFactory.getLogger(WorkManagerImpl.class);

		agentList = new HashMap<String, WorkAgent>();
		this.app = app;
		Map params = app.getParams();
		String managerList = (String)params.get(MANAGE_CONFIG);		
		try {
			for (String managerName : managerList.split(",") ) {
				Class<?> agentClass = Class.forName(managerName);			
				WorkAgent agent = (WorkAgent) agentClass.newInstance();
				agent.init(this);
				agentList.put(agent.getName(), agent);
			}
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
		if ( agentList.containsKey(name))
			return agentList.get(name);
		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {
		
		jobScan();
		workPoll();
		if ( name.length() != 0 ) {
			throw new FileNotFoundException();
		}
		Map out = new HashMap();
		Map aMap = new HashMap();
		for ( WorkAgent agent : agentList.values() ) { 
			aMap.put(agent.getName(), agent.getWorkTypes() );
		}
		out.put( "agents", aMap );

		Map<String,Integer> wMap = new HashMap<String,Integer>();
		for ( WorkStatus work : workMap.keySet() ) {
			String workType = work.getApplet().getType();
			if ( ! wMap.containsKey( workType )  ) {
				wMap.put(workType, 1);
			} else {
				wMap.put(workType, wMap.get(workType) + 1 );
			}
		}
		out.put("workCounts", wMap );
		
		try {
			os.write( JSON.dumps(out).getBytes() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void doDelete(String name, Map params, String workerID)
			throws FileNotFoundException {
	}


	private Map<WorkStatus, WorkAgent> workMap;

	@Override
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
		for ( RemusPipelineImpl pipeline : app.getPipelines() ) {			
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
		for ( WorkAgent agent : agentList.values() ) {
			agent.workPoll();
		}
	}

	@Override
	public Map getParams() {
		return app.getParams();
	}


}
