package org.remus;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RemusPipeline {

	boolean dynamic = false;
	Map<RemusApplet,NodeStatus> members;
	Map<InputReference, RemusApplet> inputs;
	CodeManager parent;
	
	List<RemusInstance> jobs;
	class NodeInstanceStatus {
		LinkedList<Integer> jobsRemaining;
	}
	
	class NodeStatus {
		Map<RemusInstance, NodeInstanceStatus> instance;
		NodeStatus() {
			instance = new HashMap<RemusInstance, NodeInstanceStatus>();
		}
	}
	
	public RemusPipeline(CodeManager parent) {
		this.parent = parent;
		members = new HashMap<RemusApplet,NodeStatus>();
		jobs = new LinkedList<RemusInstance>();
		inputs = new HashMap<InputReference, RemusApplet >();
	}

	public void addApplet(RemusApplet applet) {
		for ( InputReference iref : applet.getInputs() ) {
			if ( iref.dynamicInput ) {
				dynamic = true;
			}
		}
		inputs = null; //invalidate input list
		members.put(applet, new NodeStatus() );
	}

	public List<RemusWork> getWorkQueue( RemusInstance job, int maxCount ) {
		List<RemusWork> out = new LinkedList<RemusWork>();
		for ( RemusApplet applet : members.keySet() ) {
			NodeStatus status = members.get(applet);
			if ( status.instance.containsKey(job) ) {
				for ( Integer jobID : status.instance.get(job).jobsRemaining ) {
					if ( out.size() < maxCount ) {
						out.add( new RemusWork(this, applet, jobID) );
					}
				}
			}
		}
		return out;
	}
	
	private void setInputs() {
		inputs = new HashMap<InputReference, RemusApplet>();
		for ( RemusApplet applet : members.keySet() ) {
			for ( InputReference iref : applet.getInputs() ) {
				if ( iref.dynamicInput || !iref.isLocal() || !parent.containsKey( iref.getPath() )) {
					inputs.put(iref, applet);
				}
			}
		}
	}
	
	public List<RemusWork> getWorkQueue(int maxCount) {
		if ( inputs == null ) {
			setInputs();
		}
		List<RemusWork> out = new LinkedList<RemusWork>();
		if ( dynamic ) {
			for ( RemusInstance job : jobs ) {
				if ( out.size() < maxCount ) {
					out.addAll( getWorkQueue( job, maxCount - out.size() ) );
				}
			}
		} else {
			if ( jobs.size() > 0 ) {
				out.addAll( getWorkQueue( jobs.get(0), maxCount - out.size() ) );
			}
		}		
		return out;
	}

	public Set<RemusApplet> getMembers() {
		return members.keySet();
	}

	public Set<InputReference> getInputs() {
		if ( inputs == null ) {
			setInputs();
		}
		
		return inputs.keySet();
	}

	public void addInstance(RemusInstance instance) {
		if ( inputs == null ) {
			setInputs();
		}		
		for ( InputReference iref : inputs.keySet() ) {
			NodeInstanceStatus status = new NodeInstanceStatus();
			status.jobsRemaining = new LinkedList<Integer>();
			status.jobsRemaining.add(0);
			members.get( inputs.get(iref) ).instance.put(instance, status);
		}
		jobs.add(instance);
	}

}
