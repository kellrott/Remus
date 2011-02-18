package org.remus;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Collection;
import java.util.Map;

public class WorkManager {

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
		
		LinkedList<WorkDescription> wList = workerSets.get(workerID);
		if ( wList.size() == 0 ) {
			wList.add( workQueue.removeFirst() );
		}	
		
		return wList;
	}
	
	public WorkDescription checkoutWork( String workerID  ) {
		lastAccess.put(workerID, new Date() );
		
		
		return null;
	}
	
	
}
