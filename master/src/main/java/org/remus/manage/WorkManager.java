package org.remus.manage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.PeerInfo;
import org.remus.RemusDatabaseException;
import org.remus.RemusManager;
import org.remus.core.AppletInstance;
import org.remus.core.RemusApp;
import org.remus.core.RemusApplet;
import org.remus.core.RemusPipeline;
import org.remus.core.WorkStatus;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.JobStatus;
import org.remus.thrift.NotImplemented;
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

	Logger logger;

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
		workerSets = new HashMap<String, Set<Long>>();
		assignRate = 1;


		//lastAccess = new HashMap<String, Date>();
		finishTimes = new HashMap<String, Date>();
	}

	PluginManager plugins;
	@Override
	public void start(PluginManager pluginManager) throws Exception {
		plugins = pluginManager;
		sThread = new ScheduleThread();
		sThread.start();
	}


	/**
	 * A map of active applet instances to the peerIDs
	 * of the workers doing jobs for this applet
	 */
	private Map<AppletInstance,Set<String>> activeStacks;

	/**
	 * A map of worker peerIDs to jobsIDs
	 */
	private Map<String,Set<String>> activeWork;

	/**
	 * 
	 */

	private Map<String,Set<Long>> workerSets;
	//private Map<String,Date> lastAccess;
	private int assignRate;
	private HashMap<String, Date> finishTimes;

	public static final int MAX_REFRESH_TIME = 30 * 1000;

	private ScheduleThread sThread;

	private class ScheduleThread extends Thread {
		boolean quit = false;

		@Override
		public void run() {

			while (!quit) {
				//First, scan all of the stacks to find active worksets
				scanJobs();

				//scan the workers, start assigning untouched work to workers that aren't over limit
				workSchedule();

				//collect and clean finished jobs
				Boolean jobsDone = cleanJobs();

				try {
					if (!jobsDone) {
						sleep(10000);
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		public void quit() {
			quit = true;
		}

	}

	private void scanJobs() {
		Set<AppletInstance> fullList = new HashSet();
		try {
			RemusApp app = new RemusApp(plugins.getDataServer(), plugins.getAttachStore());
			for (String name : app.getPipelines()) {
				RemusPipeline pipe = app.getPipeline(name);
				Set<AppletInstance> curSet = pipe.getActiveApplets();
				fullList.addAll(curSet);
			}
			if (fullList.size() > 0) {
				logger.info("MANAGER found " + fullList.size() + " active stacks");
			}
		} catch (RemusDatabaseException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}

		//merge the found work with work that has been already assigned

	}

	private void workSchedule() {

	}

	private void workAssign(AppletInstance ai, String peerID, int assignRate) {
		logger.info("Assigning " + ai + " to " + peerID);
		WorkDesc wdesc = new WorkDesc();
		wdesc.workStack = new AppletRef(
				ai.getPipeline().getID(), 
				ai.getInstance().toString(), 
				ai.getApplet().getID());
		wdesc.lang = ai.getApplet().getType();
		try {
			wdesc.infoJSON = JSON.dumps(ai.getInstanceInfo());

			switch (ai.getApplet().getMode()) {
			case RemusApplet.MAPPER: {
				wdesc.mode = WorkMode.MAP;
				break;
			}
			case RemusApplet.REDUCER: {
				wdesc.mode = WorkMode.REDUCE;
				break;
			}
			case RemusApplet.SPLITTER: {
				wdesc.mode = WorkMode.SPLIT;
				break;
			}
			case RemusApplet.MATCHER: {
				wdesc.mode = WorkMode.MATCH;
				break;						
			}
			case RemusApplet.MERGER: {
				wdesc.mode = WorkMode.MERGE;
				break;						
			}
			case RemusApplet.PIPE: {
				wdesc.mode = WorkMode.PIPE;
				break;						
			}
			default: {}
			}

			RemusNet.Iface worker = plugins.getPeer(peerID);
			Collection<Long> jobs;

			jobs = ai.getReadyJobs(assignRate);
			//logger.info("Found " + jobs.size() + " jobs");
			if (jobs.size() > 0) {
				wdesc.jobs = new ArrayList(jobs);
				String jobID = worker.jobRequest("test", wdesc);


			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private boolean cleanJobs() {
		boolean found = false;
		for (String peerID : activeWork.keySet()) {
			RemusNet.Iface worker = plugins.getPeer(peerID);
			Set<String> jobSet = activeWork.get(peerID);
			for (String jobID : jobSet ) {
				JobStatus s = worker.jobStatus(jobID);
				if (s == JobStatus.DONE) {
					found = true;
					for (long job : jobs) {
						stat.finishJob(job, peerID);
					}
				} else if (s == JobStatus.ERROR) {
					found = true;
					for (long job : jobs) {
						stat.errorJob(job, "javascript error");
					}
				}
			}
			return found;
		}
	}

	@Override
	public void scheduleRequest() throws TException, NotImplemented {

	}

	/*
	void fillWorkerSet(String workerID) {
		do {
			if ( activeStack == null ) {
				//activeStack = parent.requestWorkStack(this, codeTypes);
				if ( activeStack == null )
					return;
			}
			if (activeStack.isComplete()) {
				//parent.completeWorkStack(activeStack);
				activeStack = null;
			}
		} while ( activeStack == null );

		if ( activeStack != null ) {
			Set<Long> workerMap = workerSets.get( workerID );
			if ( workerMap == null ) {
				workerMap = new HashSet<Long>();
				workerSets.put(workerID, workerMap);
			}
			if ( activeSet.size() + assignSet.size() < assignRate ) {
				Collection<Long> workSet = activeStack.getReadyJobs( workerSets.size() * assignRate );
				activeSet.addAll( workSet );
				activeSet.removeAll( assignSet );
			}
			while ( workerMap.size() < assignRate && activeSet.size() > 0) {
				long id = activeSet.iterator().next();
				workerMap.add( id );
				activeSet.remove( id );
				assignSet.add( id );
			}
		}
	}
	 */

	@Override
	public void stop() {
		sThread.quit();
	}

	/*
	 while ((curline = br.readLine()) != null) {
				Map m = (Map)JSON.loads( curline );
				for ( Object instObj : m.keySet() ) {
					for ( Object appletObj : ((Map)m.get(instObj)).keySet() ) {
						List jobList = (List)((Map)m.get(instObj)).get(appletObj);
						for ( Object key2 : jobList ) {
							long jobID = Long.parseLong( key2.toString() );
							//TODO:add emit id count check
							activeStack.finishJob( jobID, workerID );
							synchronized ( finishTimes ) {			
								Date d = new Date();		
								Date last = finishTimes.get( workerID );
								if ( last != null ) {
									if ( d.getTime() - last.getTime() > MAX_REFRESH_TIME ) {
										assignRate /= 2;
									}
									assignRate++;
								}
								finishTimes.put(workerID, d);
							}

						}

					}
				}
			}

	 */




}
