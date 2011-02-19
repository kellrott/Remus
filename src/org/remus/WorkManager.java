package org.remus;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Collection;
import java.util.Map;

import org.remus.applet.RemusApplet;

public class WorkManager {

	public static final int QUEUE_MAX = 10000;
	LinkedList<WorkDescription> workQueue;
	
	Map<String,LinkedList<WorkDescription>> workerSets;
	Map<String,Date> lastAccess;
	
	RemusApp app;
	public WorkManager(RemusApp app) {
		this.app = app;		
		workerSets = new HashMap<String, LinkedList<WorkDescription>>();
		lastAccess = new HashMap<String, Date>();
	}
	

	public Collection<WorkDescription> getWorkList( String workerID ) {
		lastAccess.put(workerID, new Date() );
		
		if ( !workerSets.containsKey( workerID ) ) {
			workerSets.put(workerID, new LinkedList<WorkDescription>());
		}
		
		if ( workQueue.size() == 0 ) {
			workQueue.addAll( app.codeManager.getWorkQueue(QUEUE_MAX) );
		}
		
		LinkedList<WorkDescription> wList = workerSets.get(workerID);
		if ( wList.size() == 0 ) {
			wList.add( workQueue.removeFirst() );
		}	
		
		return wList;
	}
	
	public WorkDescription finishWork( String workerID, RemusApplet applet, RemusInstance inst, int jobID  ) {
		lastAccess.put(workerID, new Date() );
		
		
		return null;
	}
	
	
}
