package org.remus.manage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.AbstractMap.SimpleEntry;

import org.mpstore.MPStore;
import org.remus.RemusApp;
import org.remus.RemusInstance;
import org.remus.work.AppletInstance;
import org.remus.work.RemusApplet;
import org.remus.work.SimpleAppletInstance;
import org.remus.work.WorkKey;

public class WorkManager {
	public static final int QUEUE_MAX = 10000;
	Map<AppletInstance,Set<WorkKey>>  workQueue;
	Map<String,Map<AppletInstance,Set<WorkKey>>> workerSets;
	Map<String,Date> lastAccess;
	LinkedList<Date> finishTimes;

	RemusApp app;
	public WorkManager(RemusApp app) {
		this.app = app;		
		workQueue = new HashMap<AppletInstance,Set<WorkKey>>();
		workerSets = new HashMap<String, Map<AppletInstance,Set<WorkKey>>>();
		lastAccess = new HashMap<String, Date>();
		finishTimes = new LinkedList<Date>();
	}


	public static final long WORKER_TIMEOUT = 5 * 60 * 1000;

	public Map<AppletInstance,Set<WorkKey>> getWorkList( String workerID, int maxCount ) {
		Date curDate = new Date();
		lastAccess.put(workerID, curDate );

		synchronized (workerSets) {			
			if ( !workerSets.containsKey( workerID ) ) {
				workerSets.put(workerID, new HashMap<AppletInstance,Set<WorkKey>>());
			}
			for ( String worker : lastAccess.keySet() ) {
				Date last = lastAccess.get(worker);
				if ( curDate.getTime() - last.getTime() > WORKER_TIMEOUT && workerSets.containsKey(worker)) {
					workerSets.remove(worker);
				}
			}
		}
		if ( workQueue.size() == 0 ) {
			Map<AppletInstance, Set<WorkKey>> newwork = app.getWorkQueue(QUEUE_MAX);
			for (AppletInstance ai : newwork.keySet() ) {
				assert ai != null;
				for ( WorkKey wk : newwork.get(ai) ) {
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
		}

		Map<AppletInstance,Set<WorkKey>> wMap = workerSets.get(workerID);
		synchronized ( workQueue ) {
			int workCount = 0;
			for ( AppletInstance ai : workQueue.keySet() ) {
				Set<WorkKey> wqSet = workQueue.get(ai);
				HashSet<WorkKey> addSet = new HashSet<WorkKey>();
				for ( WorkKey wk : wqSet ) {
					if ( workCount < maxCount ) {
						addSet.add(wk);
						workCount++;
					}
				}
				wqSet.removeAll(addSet);
				if ( !wMap.containsKey(ai) )
					wMap.put(ai, new HashSet<WorkKey>() );
				wMap.get(ai).addAll(addSet);
			}
		}
		emptyQueues();
		return workerSets.get(workerID);
	}

	private void emptyQueues() {
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

	public void errorWork( String workerID, RemusApplet applet, RemusInstance inst, int jobID, String error )	 {
		synchronized ( lastAccess ) {
			lastAccess.put(workerID, new Date() );
		}
		WorkKey ref = new WorkKey(inst, jobID);

		synchronized (workerSets) {
			workerSets.get(workerID).remove(ref);
			if ( workerSets.get(workerID).size() == 0 )
				workerSets.remove(workerID);
		}
		applet.errorWork(inst, jobID, workerID, error);		
	}



	public void finishWork( String workerID, RemusApplet applet, RemusInstance inst, int jobID, long emitCount  ) {
		Date d = new Date();
		lastAccess.put(workerID, d );
		synchronized (finishTimes) {
			finishTimes.add(d);
			while ( finishTimes.size() > 100 ) {
				finishTimes.removeFirst();
			}
		}
		AppletInstance ai = new SimpleAppletInstance(applet,inst);
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
		Map<String,Map<String,List>> out = new HashMap();
		for ( AppletInstance ai : workList.keySet() ) {
			assert ai != null;
			assert ai.applet != null;
			String appletStr = ai.applet.getPath();
			String instStr = ai.inst.toString();
			if ( i < count ) {
				Set<WorkKey> addSet = new HashSet<WorkKey>();
				for ( WorkKey wk : workList.get(ai) ) {
					if ( i < count ) {
						addSet.add(wk);
						i++;
					}
				}
				Map instMap = new HashMap();
				instMap.put(instStr, ai.formatWork(addSet) );
				if ( ! out.containsKey(instStr) ) {
					out.put( instStr, new HashMap() );
				}
				if ( ! out.get( instStr ).containsKey( appletStr ) ) {
					out.get( instStr ).put( appletStr, new ArrayList());
				}
				out.get( instStr ).get( appletStr ).add( ai.formatWork(addSet) );
			}
		}
		return out;
	}


	public long getFinishRate() {
		long count = 0;
		long sum = 0;
		synchronized ( finishTimes ) {
			for ( int i = 0; i < finishTimes.size() - 1; i++ ) {
				sum += finishTimes.get(i).getTime() - finishTimes.get(i+1).getTime();
				count++;
			}
		}
		if ( count > 0 )
			return sum / count;
		return 0;
	}

	public Collection<String> getWorkers() {
		return workerSets.keySet();
	}


}
