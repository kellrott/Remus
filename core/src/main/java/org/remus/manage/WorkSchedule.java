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
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.core.AppletInstance;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusApp;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PeerManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.JobState;
import org.remus.thrift.JobStatus;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.RemusNet;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.WorkMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkSchedule {

	private PeerManager peerManager;
	private Map<AppletInstance, Set<RemoteJob>> activeStacks;
	private Map<String, Set<RemoteJob>> peerStacks;
	private Map<AppletInstance, Integer> assignRate;
	private Logger logger;
	private SchemaEngine schemaEngine;

	
	public static final int MAX_REFRESH_TIME = 30 * 1000;

	public WorkSchedule(PeerManager peerManager, SchemaEngine schemaEngine) {
		logger = LoggerFactory.getLogger(WorkSchedule.class);
		this.peerManager = peerManager;
		activeStacks = new HashMap<AppletInstance, Set<RemoteJob>>();
		peerStacks = new HashMap<String, Set<RemoteJob>>();
		assignRate = new HashMap<AppletInstance, Integer>();
		this.schemaEngine = schemaEngine;
	}


	public boolean doSchedule() {
		boolean workChange = false;
		//First, scan all of the stacks to find active worksets
		scanJobs();
		//scan the workers, start assigning untouched work to workers that aren't over limit
		if (workSchedule()) {
			workChange = true;
		}
		//collect and clean finished jobs
		try {
			if (cleanJobs()) {
				workChange = true;
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return workChange;
	}



	private void scanJobs() {
		RemusNet.Iface db = null;
		RemusNet.Iface attach = null;
		try {
			db = peerManager.getPeer(peerManager.getDataServer());
			attach = peerManager.getPeer(peerManager.getAttachStore());
			RemusApp app = new RemusApp(RemusDB.wrap(db), RemusAttach.wrap(attach));
			int activeCount = 0;
			Set<AppletInstance> fullSet = new HashSet<AppletInstance>();
			for (String name : app.getPipelines()) {
				List<RemusInstance> instList = new LinkedList<RemusInstance>();
				RemusPipeline pipe = app.getPipeline(name);

				schemaEngine.processSubmissions(pipe);

				for (String subKey : pipe.getSubmits()) {
					PipelineSubmission subData = pipe.getSubmitData(subKey);
					instList.add(subData.getInstance());
				}
				for (RemusInstance inst : instList) {
					Set<AppletInstance> curSet = pipe.getActiveApplets(inst);
					activeCount += curSet.size();
					fullSet.addAll(curSet);
				}
				//check for work that needs to be instanced
				for ( String appletName : pipe.getMembers() ) {
					RemusApplet applet = pipe.getApplet(appletName);
					if (applet.getMode() != RemusApplet.STORE) {
						for (RemusInstance inst : instList) {
							if (!pipe.hasAppletInstance(inst, appletName)) {
								boolean inputFound = false;
								for (String input : applet.getSources()) {
									if (pipe.hasAppletInstance(inst, input)) {
										inputFound = true;
									}
									if (inputFound) {
										AppletInstance src = pipe.getAppletInstance(inst, applet.getSource());
										if (src != null) {
											PipelineSubmission info = src.getInstanceInfo();
											applet.createInstance(info, inst);
										} else {
											//TODO: init new work!!!
										}
									}
								}
							}
						}
					}
				}
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
		} finally {
			peerManager.returnPeer(db);
			peerManager.returnPeer(attach);
		}
	}

	public boolean workSchedule() {
		boolean workAdded = false;
		try {
			flushAppletWorkCache();
			Set<String> peers = peerManager.getWorkers();
			for (String peerID : peers) {
				if (!peerStacks.containsKey(peerID)) {
					peerStacks.put(peerID, new HashSet<RemoteJob>());
				}
				if (peerStacks.get(peerID).size() == 0) {
					//find an applet stack with a matching worktype					
					PeerInfoThrift pinfo = peerManager.getPeerInfo(peerID);
					AppletInstance ai = null;
					long [] workIDs = null;
					if (pinfo != null && pinfo.workTypes != null) {
						synchronized (activeStacks) {						
							for (AppletInstance acur : activeStacks.keySet()) {	
								if (ai == null && acur.getApplet() != null) {
									if (pinfo.workTypes.contains(acur.getApplet().getType())) {
										workIDs = getAppletWorkCache(acur);
										if (workIDs != null) {
											ai = acur;
										}
									}
								}
							}
						}
					}
					if (ai != null) {
						if (workIDs != null) {
							int curPos = 0;
							while (curPos < workIDs.length) {
								if (workIDs[curPos] == -1) {
									curPos++;
								} else {
									int last = curPos;
									do {
										curPos++;
									} while (curPos < workIDs.length && workIDs[curPos] - workIDs[last] == curPos - last);						
									workAssign(ai, peerID, workIDs[last], workIDs[curPos - 1] + 1);
									for (int i = last; i < curPos; i++) {
										workIDs[i] = -1;
									}
									workAdded = true;
								}
							}
						}
					}
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return workAdded;
	}

	private void workAssign(AppletInstance ai, String peerID, long workStart, long workEnd) {
		try {
			PipelineSubmission instanceInfo = ai.getInstanceInfo();
			if (instanceInfo == null) {
				return;
			}
			WorkDesc wdesc = new WorkDesc();
			wdesc.setWorkStack(new AppletRef());		
			wdesc.workStack.setPipeline(ai.getPipeline().getID());
			wdesc.workStack.setInstance(ai.getInstance().toString());
			wdesc.workStack.setApplet(ai.getApplet().getID());
			wdesc.setWorkStart(workStart);
			wdesc.setWorkEnd(workEnd);
			wdesc.setLang(ai.getApplet().getType());
			logger.info("Assigning " + ai + ":" + workStart + "-" + workEnd + " to " + peerID + " " + wdesc.workStack);
			wdesc.setInfoJSON(JSON.dumps(instanceInfo));

			RemusNet.Iface worker = peerManager.getPeer(peerID);
			if (worker == null) {
				return;
			}
			int mode = ai.getApplet().getMode();
			if (mode == WorkMode.AGENT.getValue()) {
				logger.info("Agent Operation");
				wdesc.setMode(WorkMode.MAP);				
				schemaEngine.setupAIStack();
				String jobID = worker.jobRequest(peerManager.getManager(), peerManager.getAttachStore(), wdesc);
				synchronized (activeStacks) {
					addRemoteJob(ai, new RemoteJob(peerID, jobID, workStart, workEnd));
				}				
			} else {
				wdesc.setMode(WorkMode.findByValue(mode));
				String jobID = worker.jobRequest(peerManager.getDataServer(), peerManager.getAttachStore(), wdesc);
				synchronized (activeStacks) {
					addRemoteJob(ai, new RemoteJob(peerID, jobID, workStart, workEnd));
				}
			}			
			peerManager.returnPeer(worker);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemusDatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private boolean cleanJobs() throws TException, NotImplemented {
		boolean found = false;
		synchronized (activeStacks) {
			int activeCount = 0;
			for (AppletInstance ai : activeStacks.keySet()) {			
				Map<RemoteJob, Boolean> removeSet = new HashMap<RemoteJob, Boolean>();
				for (RemoteJob rj : activeStacks.get(ai)) {
					logger.debug("Checking " + rj.getPeerID() + " for " + rj.getJobID() + " " + ai);
					try {
						RemusNet.Iface worker = peerManager.getPeer(rj.getPeerID());
						if (worker != null) {
							JobStatus s = worker.jobStatus(rj.getJobID());
							if (s.status == JobState.DONE) {
								found = true;
								for (long i = rj.getWorkStart(); i < rj.getWorkEnd(); i++) {
									ai.finishWork(i, rj.getPeerID(), s.emitCount);
								}
								removeSet.put(rj, true);
								logger.info("Worker Finished: " + rj.getJobID());
							} else if (s.status == JobState.ERROR) {
								found = true;
								for (long i = rj.getWorkStart(); i < rj.getWorkEnd(); i++) {
									ai.errorWork(i, s.errorMsg);
								}
								removeSet.put(rj, false);
								logger.warn("JOB ERROR: " + ai + " job:" + rj.getJobID() + " " + s.errorMsg);
							} else if (s.status == JobState.UNKNOWN) {
								logger.warn("Worker lost job: " + rj.getJobID());
							} else {
								activeCount += 1;
							}
							peerManager.returnPeer(worker);
						}
					} catch (TException e) {
						logger.warn("Worker Connection Failed: " + rj.getPeerID());
						removeSet.put(rj, false);
						peerManager.peerFailure(rj.getPeerID());
					}
				}
				for (RemoteJob rj : removeSet.keySet()) {
					removeRemoteJob(ai, rj, removeSet.get(rj));
				}
			}
			if (activeCount > 0) {
				logger.info("Active RemoteJobs: " + activeCount);			
			}
		}
		return found;
	}





	private long [] getAppletWorkCache(AppletInstance ai) throws NotImplemented, TException {
		Set<String> peers = peerManager.getWorkers();
		long [] workIDs = null;
		synchronized (workIDCache) {			
			if (workIDCache.containsKey(ai) && workIDCache.get(ai) != null) {
				workIDs = workIDCache.get(ai);
				Arrays.sort(workIDs);
			} else {
				workIDs = ai.getReadyJobs(assignRate.get(ai) * peers.size());				
				workIDCache.put(ai, workIDs);
			}
		}
		for (String peerID : peers) {
			if (!peerStacks.containsKey(peerID)) {
				peerStacks.put(peerID, new HashSet<RemoteJob>());
			}
			for (RemoteJob rj : peerStacks.get(peerID)) {
				if (activeStacks.get(ai).contains(rj)) {
					for (int i = 0; i < workIDs.length; i++) {
						if (workIDs[i] >= rj.getWorkStart() && workIDs[i] < rj.getWorkEnd()) {
							workIDs[i] = -1;
						}
					}
				}
			}
		}

		boolean valid = false;
		for (int i = 0; i < workIDs.length; i++) {
			if (workIDs[i] != -1) {
				valid = true;
			}
		}
		if (valid) {
			Arrays.sort(workIDs);
			return workIDs;
		} 
		workIDCache.put(ai, new long [0]);
		return null;
	}



	private Map<AppletInstance,long []> workIDCache = new HashMap<AppletInstance, long[]>();

	private void flushAppletWorkCache() {
		synchronized (workIDCache) {			
			List<AppletInstance> removeList = new LinkedList<AppletInstance>();
			for (AppletInstance ai : workIDCache.keySet()) {
				if (workIDCache.get(ai) == null || workIDCache.get(ai).length == 0) {
					removeList.add(ai);
				}
			}
			for (AppletInstance ai : removeList) {
				workIDCache.remove(ai);
			}
		}
	}



	private void syncAppletList(Set<AppletInstance> aiSet) {
		Set<AppletInstance> removeSet = new HashSet<AppletInstance>();		
		synchronized (activeStacks) {		
			for (AppletInstance ai : aiSet) {
				if (!activeStacks.containsKey(ai)) {
					activeStacks.put(ai, new HashSet<RemoteJob>());
					assignRate.put(ai, 1);
				}
			}
			for (AppletInstance ai : activeStacks.keySet()) {
				if (!aiSet.contains(ai)) {
					removeSet.add(ai);
				}
			}
		}

		for (AppletInstance ai : removeSet) {
			for (RemoteJob rj : activeStacks.get(ai)) {
				removeRemoteJob(ai, rj, false);
			}
			synchronized (activeStacks) {
				activeStacks.remove(ai);				
			}
			synchronized (workIDCache) {
				workIDCache.remove(ai);				
			}
		}

	}

	private void addRemoteJob(AppletInstance ai, RemoteJob rj) {
		activeStacks.get(ai).add(rj);
		if (!peerStacks.containsKey(rj.getPeerID())) {
			peerStacks.put(rj.getPeerID(), new HashSet<RemoteJob>());
		}
		peerStacks.get(rj.getPeerID()).add(rj);
	}

	private void removeRemoteJob(AppletInstance ai, RemoteJob rj, boolean adjustAssignRate) {
		if (adjustAssignRate) {
			int newAssignRate = assignRate.get(ai);
			long runTime = (new Date()).getTime() - rj.getStartTime();
			if (runTime > MAX_REFRESH_TIME) {
				newAssignRate /= 2;
			}
			newAssignRate++;
			logger.info("ASSIGN RATE: " + ai + " " + newAssignRate);
			assignRate.put(ai, newAssignRate);
		}
		try {
			RemusNet.Iface worker = peerManager.getPeer(rj.getPeerID());					
			worker.jobCancel(rj.getJobID());	
			peerManager.returnPeer(worker);
		} catch (NotImplemented e) {
			e.printStackTrace();
		} catch (TException e) {
			peerManager.peerFailure(rj.getPeerID());
			e.printStackTrace();
		}
		peerStacks.get(rj.getPeerID()).remove(rj);
		activeStacks.get(ai).remove(rj);
	}


	public Object getScheduleInfo() {
		Map out = new HashMap();
		synchronized (activeStacks) {
			int outCount = activeStacks.size();
			out.put("activeCount", Integer.toString(outCount));			
			Map aiMap = new HashMap();
			for (AppletInstance ai : activeStacks.keySet()) {
				List o = new LinkedList();
				for (RemoteJob rj : activeStacks.get(ai)) {
					o.add(rj.getPeerID());
				}
				aiMap.put(ai.toString(), o);
			}
			out.put("active", aiMap);
		}		
		return out;
	}



}
