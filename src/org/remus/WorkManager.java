package org.remus;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.remus.applet.RemusApplet;

public class WorkManager {

	public static final int QUEUE_MAX = 10000;
	LinkedList<WorkDescription> workQueue;

	Map<String,Map<WorkReference, Object>> workerSets;
	Map<String,Date> lastAccess;
	LinkedList<Date> finishTimes;

	RemusApp app;
	public WorkManager(RemusApp app) {
		this.app = app;		
		workQueue = new LinkedList<WorkDescription>();
		workerSets = new HashMap<String, Map<WorkReference,Object>>();
		lastAccess = new HashMap<String, Date>();
		finishTimes = new LinkedList<Date>();
	}


	public static final long WORKER_TIMEOUT = 5 * 60 * 1000;

	public Map<WorkReference,Object> getWorkList( String workerID, int maxCount ) {
		Date curDate = new Date();
		lastAccess.put(workerID, curDate );
		if ( !workerSets.containsKey( workerID ) ) {
			workerSets.put(workerID, new HashMap<WorkReference,Object>());
		}

		synchronized (workerSets) {			
			for ( String worker : lastAccess.keySet() ) {
				Date last = lastAccess.get(worker);
				if ( curDate.getTime() - last.getTime() > WORKER_TIMEOUT && workerSets.containsKey(worker)) {
					workerSets.remove(worker);
				}
			}
		}
		if ( workQueue.size() == 0 ) {
			for (WorkDescription desc : app.codeManager.getWorkQueue(QUEUE_MAX) ) {
				boolean found = false;
				for ( Map<WorkReference,Object> worker : workerSets.values() ) {
					if ( worker.containsKey( desc.ref ) ) {
						found = true;
					}
				}
				if ( !found ) {
					synchronized (workQueue) {						
						workQueue.add( desc );
					}
				}
			}
		}
		Map<WorkReference,Object> wMap = workerSets.get(workerID);
		while ( wMap.size() < maxCount && workQueue.size() > 0 ) {
			WorkDescription desc = workQueue.removeFirst();
			wMap.put( desc.ref, desc.desc );
		}
		return wMap;
	}

	public void errorWork( String workerID, RemusApplet applet, RemusInstance inst, long jobID, String error )	 {
		lastAccess.put(workerID, new Date() );
		WorkReference ref = new WorkReference(applet, inst, jobID);
		workerSets.get(workerID).remove(ref);
		applet.errorWork(inst, jobID, workerID, error);		
	}



	public void finishWork( String workerID, RemusApplet applet, RemusInstance inst, long jobID  ) {
		Date d = new Date();
		lastAccess.put(workerID, d );
		synchronized (finishTimes) {
			finishTimes.add(d);
			while ( finishTimes.size() > 100 ) {
				finishTimes.removeFirst();
			}
		}
		WorkReference ref = new WorkReference(applet, inst, jobID);
		workerSets.get(workerID).remove(ref);
		applet.finishWork(inst, jobID, workerID);

	}


	public Object getWorkMap(String workerID, int count) {
		Map<WorkReference,Object> workList = getWorkList( workerID, count );
		Map<String,Map<?,?>> outMap = new HashMap<String,Map<?,?>>();
		int i = 0;
		for ( WorkReference work : workList.keySet() ) {
			if ( i < count ) {
				if ( !outMap.containsKey(work.instance.toString()) )
					outMap.put( work.instance.toString(), new HashMap() );
				Map iMap = outMap.get(work.instance.toString());
				if ( !iMap.containsKey( work.applet.getPath() ) )
					iMap.put( work.applet.getPath(), new ArrayList() );
				List aList = (List)iMap.get( work.applet.getPath() );
				aList.add(work.jobID);
				i++;
			}
		}
		return outMap;
	}


	public Object getWork(String workerID, RemusApplet applet, RemusInstance inst, int jobID) {
		lastAccess.put(workerID, new Date() );
		WorkReference ref = new WorkReference(applet, inst, jobID);
		return workerSets.get(workerID).get(ref);
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


}
