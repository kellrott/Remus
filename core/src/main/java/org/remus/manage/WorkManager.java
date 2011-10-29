package org.remus.manage;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.PeerInfo;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.RemusManager;
import org.remus.core.AppletInstance;
import org.remus.core.AppletInstanceStack;
import org.remus.core.BaseStackNode;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusApp;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusMiniDB;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PeerManager;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.JobState;
import org.remus.thrift.JobStatus;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.PeerType;
import org.remus.thrift.RemusNet;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.WorkMode;

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

	private RemusNet.Iface db, attach;

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

	@Override
	public void start(PluginManager pluginManager) throws Exception {
		peerManager = pluginManager.getPeerManager();
		/**
		 * The miniDB is a shim placed infront of the database, that will allow you
		 * to add additional, dynamic, applets to the database. For the work manager
		 * it is used to create the '/@agent' applets, which view all the applet instance
		 * records as a single stack
		 */
		schemaEngine = new SchemaEngine(peerManager);
		schedule = new WorkSchedule(peerManager, schemaEngine);
		
		db = peerManager.getPeer(peerManager.getDataServer());
		attach = peerManager.getPeer(peerManager.getAttachStore());			

		schemaEngine.setupAIStack();
		
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
	public void scheduleRequest() throws TException, NotImplemented {
		sThread.touch();
	}


	@Override
	public void stop() {
		sThread.quit();
	}

	@Override
	public String scheduleInfoJSON() throws NotImplemented, TException {
		Object out = schedule.getScheduleInfo();
		return JSON.dumps(out);
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
