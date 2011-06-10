package org.remus.manage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.mpstore.Serializer;
import org.remus.RemusApp;
import org.remus.RemusInstance;
import org.remus.serverNodes.BaseNode;
import org.remus.work.AppletInstance;
import org.remus.work.RemusApplet;
import org.remus.work.SimpleAppletInstance;
import org.remus.work.WorkKey;

public class WorkManager implements BaseNode {
	public static final int QUEUE_MAX = 10000;
	Map<AppletInstance,Set<WorkKey>>  workQueue;
	Map<String,Map<AppletInstance,Set<WorkKey>>> workerSets;
	Map<String,Date> lastAccess;

	public static final int MAX_REFRESH_TIME = 30 * 1000;
	Map<RemusApplet,Integer> assignRate;
	Map<RemusApplet,Date> finishTimes;

	RemusApp app;
	public WorkManager(RemusApp app) {
		this.app = app;		
		workQueue = new HashMap<AppletInstance,Set<WorkKey>>();
		workerSets = new HashMap<String, Map<AppletInstance,Set<WorkKey>>>();
		lastAccess = new HashMap<String, Date>();
		finishTimes = new HashMap<RemusApplet,Date>();
		assignRate = new HashMap<RemusApplet,Integer>();
	}


	public static final long WORKER_TIMEOUT = 5 * 60 * 1000;

	public Map<AppletInstance,Set<WorkKey>> getWorkList( String workerID, int maxCount ) {
		Date curDate = new Date();
		synchronized ( lastAccess ) {
			lastAccess.put(workerID, curDate );
			synchronized (workerSets) {			
				//if worker id isn't setup, do so now
				if ( !workerSets.containsKey( workerID ) ) {
					workerSets.put(workerID, new HashMap<AppletInstance,Set<WorkKey>>());
				}
				//release work from an worker that hasn't checked in within the time limit
				for ( String worker : lastAccess.keySet() ) {
					Date last = lastAccess.get(worker);
					if ( curDate.getTime() - last.getTime() > WORKER_TIMEOUT && workerSets.containsKey(worker)) {
						workerSets.remove(worker);
					}
				}
			}
		}		
		//scan applets for new work 
		synchronized (workQueue) {			
			if ( workQueue.size() == 0 ) {
				int retry = 3;
				do {
					Map<AppletInstance, Set<WorkKey>> newwork = app.getWorkQueue(QUEUE_MAX);
					if ( newwork.size() == 0 )
						retry--;
					else
						retry = 0;
					for (AppletInstance ai : newwork.keySet() ) {
						assert ai != null;
						for ( WorkKey wk : newwork.get(ai) ) {
							//make sure that it hasn't been assigned to workers yet
							boolean found = false;
							for ( Map<AppletInstance, Set<WorkKey> > worker : workerSets.values() ) {
								if ( worker.containsKey( ai ) && worker.get(ai).contains(wk) ) {
									found = true;
								}
							}
							if ( !found ) {
								synchronized (workQueue) {		
									if ( !workQueue.containsKey(ai) ) {
										workQueue.put(ai, new HashSet<WorkKey>() );
									}
									workQueue.get(ai).add(wk);
								}
							}
						}
					}
				} while (retry > 0 );
			}
		}
		//add jobs to worker's queue 
		Map<AppletInstance,Set<WorkKey>> wMap = workerSets.get(workerID);
		synchronized ( workQueue ) {
			Map<RemusApplet,Integer> workCount = new HashMap<RemusApplet,Integer>();
			//get current counts for every applet type
			for ( AppletInstance ai : wMap.keySet() ) {
				Integer wc = workCount.get(ai.applet); 
				if ( wc == null )
					wc = 0;
				workCount.put(ai.applet, wc + wMap.get(ai).size() );
			}
			for ( AppletInstance ai : workQueue.keySet() ) {
				//System.out.println("WorkCount:" + workCount );
				Set<WorkKey> wqSet = workQueue.get(ai);
				HashSet<WorkKey> addSet = new HashSet<WorkKey>();
				int maxAssign = 1;
				if ( !assignRate.containsKey(ai) ) {
					assignRate.put(ai.applet, 1);
				} else {
					maxAssign = assignRate.get(ai);
				}
				Integer wc = workCount.get(ai.applet);
				if ( wc == null )
					wc = 0;
				for ( WorkKey wk : wqSet ) {
					if ( wc < maxAssign ) {
						//System.out.println( "Adding: " + ai + " " + wk );
						addSet.add(wk);
						wc++;
					}
				}
				if ( addSet.size() > 0 ) {
					workCount.put(ai.applet,wc);
					wqSet.removeAll(addSet);
					if ( !wMap.containsKey(ai) )
						wMap.put(ai, new HashSet<WorkKey>() );
					wMap.get(ai).addAll(addSet);
				}
			}
		}
		emptyQueues();
		return workerSets.get(workerID);
	}

	private void emptyQueues() {
		synchronized (workQueue) {
			Set<AppletInstance> rmSet = new HashSet<AppletInstance>();
			for ( AppletInstance ai : workQueue.keySet() ) {	
				Set<WorkKey> wqSet = workQueue.get(ai);
				if ( wqSet.size() == 0)
					rmSet.add(ai);
			}
			for (AppletInstance ai : rmSet){
				workQueue.remove(ai);			
			}				
		}
	}

	public void errorWork( String workerID, RemusApplet applet, RemusInstance inst, int jobID, String error )	 {
		synchronized ( lastAccess ) {
			lastAccess.put(workerID, new Date() );
		}
		WorkKey ref = new WorkKey(inst, jobID);

		synchronized (workerSets) {
			AppletInstance ai = new SimpleAppletInstance(applet,inst);
			if ( workerSets.containsKey(workerID) && workerSets.get(workerID).containsKey(ai) )
				workerSets.get(workerID).get(ai).remove(ref);
		}
		applet.errorWork(inst, jobID, workerID, error);		
	}

	public boolean hasWork(String workerID, RemusApplet applet, RemusInstance inst,	int jobID) {
		AppletInstance ai = new SimpleAppletInstance(applet,inst);	
		WorkKey ref = new WorkKey(inst, jobID);
		synchronized (workerSets) {
			if ( !workerSets.containsKey(workerID) || !workerSets.get(workerID).containsKey(ai) || !workerSets.get(workerID).get(ai).contains(ref) )
				return false;
		}
		return true;
	}

	public void finishWork( String workerID, RemusApplet applet, RemusInstance inst, int jobID, long emitCount  ) {
		Date d = new Date();		
		synchronized (lastAccess) {
			lastAccess.put(workerID, d );			
		}
		AppletInstance ai = new SimpleAppletInstance(applet,inst);
		synchronized (finishTimes) {
			Date last = finishTimes.get(ai);
			synchronized ( assignRate ) {			
				if ( last != null ) {
					if ( d.getTime() - last.getTime() < MAX_REFRESH_TIME ) {
						assignRate.put(ai.applet, assignRate.get(ai) + 1);
					} else {
						assignRate.put(ai.applet, Math.max(1, assignRate.get(ai) / 2) );
					}
				}
			}
			finishTimes.put(ai.applet,d);
		}
		WorkKey ref = new WorkKey(inst, jobID);
		synchronized (workerSets) {
			workerSets.get(workerID).get(ai).remove(ref);
			if ( workerSets.get(workerID).get(ai).size() == 0 )
				workerSets.get(workerID).remove(ai);			
		}
		applet.finishWork(inst, jobID, workerID, emitCount);
	}


	public Object getWorkMap(String workerID, int count) {
		Map<AppletInstance,Set<WorkKey>> workList = getWorkList( workerID, count );						
		int i = 0;
		Map out = new HashMap();
		for ( AppletInstance ai : workList.keySet() ) {
			assert ai != null;
			assert ai.applet != null;

			String pipelineID = ai.applet.getPipeline().getID();
			Map pipeMap = null;
			if (out.containsKey(pipelineID)) {
				pipeMap = (Map) out.get(pipelineID);
			} else {
				pipeMap = new HashMap();
			}

			String appletID = ai.applet.getID();
			String instStr = ai.inst.toString();
			if ( i < count ) {
				Set<WorkKey> addSet = new HashSet<WorkKey>();
				for ( WorkKey wk : workList.get(ai) ) {
					if ( i < count ) {
						addSet.add(wk);
						i++;
					}
				}
				//Map instMap = new HashMap();
				//instMap.put(instStr, ai.formatWork(addSet) );
				if ( addSet.size() > 0 ) {
					if ( ! pipeMap.containsKey(instStr) ) {
						pipeMap.put( instStr, new HashMap() );
					}
					if ( ! ((Map)pipeMap.get( instStr )).containsKey( appletID ) ) {
						((Map)pipeMap.get( instStr )).put( appletID, new HashMap());
					}
					Map workMap = (Map)ai.formatWork(addSet);
					for ( Object key : workMap.keySet() ) {
						((HashMap)((Map)pipeMap.get( instStr )).get( appletID )).put( key, workMap.get(key) );
					}
				}
			}
			if ( !pipeMap.isEmpty() ) {
				out.put(pipelineID, pipeMap);
			}
		}
		return out;
	}


	public Collection<String> getWorkers() {
		return workerSets.keySet();
	}

	public int getWorkerActiveCount(String workerID) {
		int count = 0;
		for ( Set<WorkKey> set : workerSets.get(workerID).values() ) {
			count += set.size();
		}
		return count;
	}

	public void touchWorkerStatus(String workerID) {
		synchronized ( lastAccess ) {
			lastAccess.put(workerID, new Date() );
		}		
	}

	public Date getLastAccess(String workerID) {
		return lastAccess.get(workerID);
	}

	public int getWorkBufferSize() {
		int count = 0;
		synchronized (workQueue) {			
			for ( AppletInstance ai : workQueue.keySet() ) {
				count += workQueue.get(ai).size();	
			}
		}
		return count;
	}

	public Map<RemusApplet,Integer> getAssignRateMap() {
		return assignRate;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID, Serializer serial,
			OutputStream os) throws FileNotFoundException {

		if ( workerID != null ) {
			Object outVal = app.getWorkManager().getWorkMap( workerID, 10 );
			try {
				os.write( serial.dumps(outVal).getBytes() );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			throw new FileNotFoundException();
		}


	}

	@Override
	public void doPut(String name, String workerID, Serializer serial, InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		try {

			BufferedReader br = new BufferedReader( new InputStreamReader(is) );
			String curline = null;
			while ((curline=br.readLine())!= null ) {
				Map m = (Map)serial.loads( curline );
				//System.out.println( curline );
				for ( Object instObj : m.keySet() ) {
					RemusInstance inst=new RemusInstance((String)instObj);
					for ( Object appletObj : ((Map)m.get(instObj)).keySet() ) {
						String appletStr = (String)appletObj;
						List jobList = (List)((Map)m.get(instObj)).get(appletObj);
						RemusApplet applet = app.getApplet( appletStr );
						for ( Object key2 : jobList ) {
							long jobID = Long.parseLong( key2.toString() );
							//TODO:add emit id count check
							app.getWorkManager().finishWork(workerID, applet, inst, (int)jobID, 0L);
						}						
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}


}
