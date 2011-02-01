package org.remus.applet;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.remus.RemusInstance;
import org.remus.WorkDescription;

public class InstanceStatus {

	WorkGenerator workGen;
	RemusApplet applet;
	InstanceStatus(RemusApplet applet, WorkGenerator workGen) {
		instance = new HashMap<RemusInstance, NodeInstanceStatus>();
		this.workGen = workGen;
		this.applet = applet;
	}

	public class NodeInstanceStatus {
		public Map<Long,WorkDescription> jobsRemaining = null;
	}

	private Map<RemusInstance, NodeInstanceStatus> instance;


	public boolean hasInstance(RemusInstance remusInstance) {
		return instance.containsKey(remusInstance);
	}

	public void addInstance(RemusInstance remusInstance) {
		NodeInstanceStatus status = new NodeInstanceStatus();
		instance.put(remusInstance, status);
	}

	public void removeWork(RemusInstance remusInstance, long jobID) {
		instance.get(remusInstance).jobsRemaining.remove(jobID);
	}

	public int jobCount(RemusInstance remusInstance) {
		checkInstance(remusInstance);
		return instance.get(remusInstance).jobsRemaining.size();
	}

	private void checkInstance(RemusInstance remusInstance) {
		if ( instance.get(remusInstance).jobsRemaining == null ) {		
			if ( !applet.isComplete(remusInstance) ) {
				if ( applet.isReady(remusInstance)) {
					workGen.startWork(remusInstance);
					Map<Long,WorkDescription> workMap = new HashMap<Long, WorkDescription>();
					WorkDescription curWork = null;
					while ( (curWork=workGen.nextWork()) != null) {
						workMap.put((long)curWork.jobID, curWork);
					}
				}
			}
		}
	}

	public WorkDescription getWork(RemusInstance inst, long jobID) {
		return instance.get(inst).jobsRemaining.get(jobID);		
	}

	public Collection<RemusInstance> getInstanceList() {
		return instance.keySet();
	}
	
	public Collection<WorkDescription> getWorkList(RemusInstance job) {
		checkInstance(job);
		if ( instance.containsKey(job) )
			return instance.get(job).jobsRemaining.values();		
		return Arrays.asList();
	}

}
