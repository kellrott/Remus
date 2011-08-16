package org.remus.manage;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.PeerInfo;
import org.remus.RemusDatabaseException;
import org.remus.RemusManager;
import org.remus.core.AppletInstance;
import org.remus.core.RemusApp;
import org.remus.core.RemusApplet;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.JobState;
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


	public static int INACTIVE_SLEEP_TIME = 10000;

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
		activeStacks = new HashMap<AppletInstance, Set<RemoteJob>>();
		peerStacks = new HashMap<String, Set<RemoteJob>>();
		assignRate = new HashMap<AppletInstance, Integer>();
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




	private class RemoteJob {
		private String peerID;
		private String jobID;
		private long workStart;
		private long workEnd;
		private long startTime;

		RemoteJob(String peerID, String jobID, long workStart, long workEnd) {
			this.peerID = peerID;
			this.jobID = jobID;
			this.workStart = workStart;
			this.workEnd = workEnd;
			this.startTime = (new Date()).getTime();
		}

		@Override
		public int hashCode() {
			return peerID.hashCode() + jobID.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof RemoteJob) {
				return equals((RemoteJob) obj);
			}
			return super.equals(obj);
		}

		public boolean equals(RemoteJob job) {
			if (job.peerID.equals(peerID) && job.jobID.equals(jobID)) {
				return true;
			}
			return false;
		}

	}

	/**
	 * A map of active applet instances to the peerIDs
	 * of the workers doing jobs for this applet.
	 */
	private Map<AppletInstance, Set<RemoteJob>> activeStacks;
	private Map<String, Set<RemoteJob>> peerStacks;
	private Map<AppletInstance, Integer> assignRate;

	private HashMap<String, Date> finishTimes;


	private void syncAppletList(Set<AppletInstance> aiSet) {
		for (AppletInstance ai : aiSet) {
			if (!activeStacks.containsKey(ai)) {
				activeStacks.put(ai, new HashSet<RemoteJob>());
				assignRate.put(ai, 1);
			}
		}
		Set<AppletInstance> removeSet = new HashSet<AppletInstance>();
		for (AppletInstance ai : activeStacks.keySet()) {
			if (!aiSet.contains(ai)) {
				removeSet.add(ai);
			}
		}
		for (AppletInstance ai : removeSet) {
			activeStacks.remove(ai);
		}
	}

	private void addRemoteJob(AppletInstance ai, RemoteJob rj) {
		activeStacks.get(ai).add(rj);
		if (!peerStacks.containsKey(rj.peerID)) {
			peerStacks.put(rj.peerID, new HashSet<RemoteJob>());
		}
		peerStacks.get(rj.peerID).add(rj);
	}

	private void removeRemoteJob(AppletInstance ai, RemoteJob rj, boolean adjustAssignRate) {
		activeStacks.get(ai).remove(rj);
	}



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
				Boolean jobsDone = false;
				try {
					jobsDone = cleanJobs();
				} catch (TException e) {
					e.printStackTrace();
				} catch (NotImplemented e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					if (!jobsDone) {
						sleep(INACTIVE_SLEEP_TIME);
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
		try {
			RemusApp app = new RemusApp(plugins.getDataServer(), plugins.getAttachStore());
			int activeCount = 0;
			Set<AppletInstance> fullSet = new HashSet<AppletInstance>();
			for (String name : app.getPipelines()) {
				RemusPipeline pipe = app.getPipeline(name);
				Set<AppletInstance> curSet = pipe.getActiveApplets();
				activeCount += curSet.size();
				fullSet.addAll(curSet);
			}
			syncAppletList(fullSet);
			if (activeCount > 0) {
				logger.info("MANAGER found " + activeCount + " active stacks");
			}
		} catch (RemusDatabaseException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}

	private boolean cleanJobs() throws TException, NotImplemented {
		boolean found = false;
		for (AppletInstance ai : activeStacks.keySet()) {			
			for (RemoteJob rj : activeStacks.get(ai)) {
				RemusNet.Iface worker = plugins.getPeer(rj.peerID);
				JobStatus s = worker.jobStatus(rj.jobID);
				if (s.status == JobState.DONE) {
					found = true;
					for (long i = rj.workStart; i < rj.workEnd; i++) {
						ai.finishWork(i, rj.peerID, s.emitCount);
					}
					removeRemoteJob(ai, rj, true);
				} else if (s.status == JobState.ERROR) {
					found = true;
					for (long i = rj.workStart; i < rj.workEnd; i++) {
						ai.errorWork(i, s.errorMsg);
					}
					removeRemoteJob(ai, rj, false);
				}
			}
		}
		return found;
	}

	private void workSchedule() {
		for (AppletInstance ai : activeStacks.keySet()) {	
			try {
				String workType = ai.getApplet().getType();
				Set<String> peers = plugins.getWorkers(workType);			
				long [] workIDs = ai.getReadyJobs(assignRate.get(ai) * peers.size());

				final HashMap<String, Long> peerBase = new HashMap<String, Long>();
				TreeMap<String, Long> peerCount = new TreeMap<String, Long>(new Comparator<String>() {
					@Override
					public int compare(String o1, String o2) {
						return (int) (peerBase.get(o1) - peerBase.get(o2));
					}
				});
				
				for (String peerID : peers) {
					long workCount = 0;
					for (RemoteJob rj : peerStacks.get(peerID)) {
						workCount += rj.workEnd - rj.workStart;
						for (int i = 0; i < workIDs.length; i++) {
							if (workIDs[i] >= rj.workStart && workIDs[i] < rj.workEnd) {
								workIDs[i] = -1;
							}
						}
					}
					peerBase.put(peerID, workCount);
				}
				peerCount.putAll(peerBase);
				
				Arrays.sort(workIDs);
				int curPos = 0;
				do {
					if (workIDs[curPos] == -1) {
						curPos++;
					} else {
						int last = curPos;
						do {
							curPos++;
						} while (curPos < workIDs.length && workIDs[curPos] - workIDs[last] == curPos - last);						
						String peerID = peerCount.firstKey();
						workAssign(ai, peerID, last, curPos);
						peerBase.put(peerID, peerBase.get(peerID) + (curPos - last));
						peerCount.clear();
						peerCount.putAll(peerBase);
					}
				} while (curPos < workIDs.length);
				

			} catch (TException e) {
				e.printStackTrace();
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}




	private void workAssign(AppletInstance ai, String peerID, long workStart, long workEnd) {
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

			wdesc.workStart = workStart;
			wdesc.workEnd = workEnd;
			String jobID = worker.jobRequest("test", wdesc);



		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
