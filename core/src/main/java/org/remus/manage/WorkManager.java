package org.remus.manage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.PeerInfo;
import org.remus.RemusManager;
import org.remus.core.BaseStackNode;
import org.remus.plugin.PeerManager;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
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

	/**
	 * A map of active applet instances to the peerIDs
	 * of the workers doing jobs for this applet.
	 */

	private Logger logger;

	PeerManager peerManager;
	private WorkSchedule schedule;
	private SchemaEngine schemaEngine;

	public static int INACTIVE_SLEEP_TIME = 30000;

	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.name = "Work Manager";
		out.peerType = PeerType.MANAGER;
		return out;
	}

	@Override
	public void init(Map params) throws Exception {
		logger = LoggerFactory.getLogger(WorkManager.class);	
	}

	
	class WorkStatusStack implements BaseStackNode {

		@Override
		public void add(String key, String data) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean containsKey(String key) {
			return schedule.workerMap.containsKey(key);
		}

		@Override
		public List<String> getValueJSON(String key) {
			return Arrays.asList(JSON.dumps(schedule.workerMap.get(key)));
		}

		@Override
		public List<String> keySlice(String keyStart, int count) {
			SortedMap<String, InstanceWorker> set = schedule.workerMap.tailMap(keyStart);
			List<String> out = new ArrayList<String>(count);
			for (String key : set.keySet()) {
				if (out.size() < count) {
					out.add(key);
				}
			}
			return out;
		}

		@Override
		public void delete(String key) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	@Override
	public void start(PluginManager pluginManager) throws Exception {
		peerManager = pluginManager.getPeerManager();
		schemaEngine = new SchemaEngine(peerManager);
		schemaEngine.setupAIStack();
		schemaEngine.addWorkStatus( new WorkStatusStack() );
		schedule = new WorkSchedule(peerManager, schemaEngine);		
		sThread = new ScheduleThread();
		sThread.start();
	}


	private ScheduleThread sThread;

	private class ScheduleThread extends Thread {

		public ScheduleThread() {
			super("Manager Schedule Thread");
		}

		boolean quit = false;
		Integer waitLock = new Integer(0);
		private int sleepTime = 300;

		@Override
		public void run() {
			while (!quit) {
				Boolean workChange = false;
				try {
					workChange = schedule.doSchedule();
				} catch (Exception e) {
					e.printStackTrace();
					logger.error(e.getMessage());
				}
				try {
					if (!workChange) {
						if (sleepTime < INACTIVE_SLEEP_TIME) {
							sleepTime += 1000;
						}
					} else {
						sleepTime = 100;
					}
					logger.debug("Manager SleepTime=" + sleepTime);
					synchronized (waitLock) {
						waitLock.wait(sleepTime);
					}					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		public void touch() {
			synchronized (waitLock) {
				waitLock.notifyAll();	
			}
		}

		public void quit() {
			quit = true;
		}

	}

	@Override
	public void stop() {
		sThread.quit();
	}
	
	@Override
	public List<String> keySlice(AppletRef stack, String keyStart, int count)
	throws NotImplemented, TException {
		logger.debug("Manage DB keySlice: " + stack + " " + keyStart + " " + count);
		return schemaEngine.keySlice(stack, keyStart, count);
	}

	@Override
	public boolean containsKey(AppletRef stack, String key)
	throws NotImplemented, TException {
		logger.debug("Manage DB containsKey: " + stack + " " + key);
		return schemaEngine.containsKey(stack, key);
	}

	@Override
	public List<String> getValueJSON(AppletRef stack, String key)
	throws NotImplemented, TException {
		logger.debug("Manage DB getValueJSON: " + stack + " " + key);
		return schemaEngine.getValueJSON(stack, key);
	}

	@Override
	public void addDataJSON(AppletRef stack, long jobID, long emitID, String key,
			String data) throws NotImplemented, TException {
		logger.debug("Manage DB add: " + stack + " " + key);
		schemaEngine.addDataJSON(stack, jobID, emitID, key, data);
		
	}

	@Override
	public String status() throws TException {
		return "OK";
	}
	
}
