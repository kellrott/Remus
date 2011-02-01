package org.remus;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.remus.applet.InstanceStatus;
import org.remus.applet.RemusApplet;
import org.remus.applet.SplitterApplet;

public class RemusPipeline {

	boolean dynamic = false;
	HashMap<String,RemusApplet> members;
	Map<InputReference, RemusApplet> inputs;
	CodeManager parent;

	List<RemusInstance> jobs;

	public RemusPipeline(CodeManager parent) {
		this.parent = parent;
		members = new HashMap<String,RemusApplet>();
		jobs = new LinkedList<RemusInstance>();
		inputs = new HashMap<InputReference, RemusApplet >();
	}

	public void addApplet(RemusApplet applet) {
		if ( applet.hasInputs() ) {
			for ( InputReference iref : applet.getInputs() ) {
				if ( iref.dynamicInput ) {
					dynamic = true;
				}
			}
		}
		applet.setPipeline( this );
		inputs = null; //invalidate input list
		members.put(applet.getPath(), applet);
	}

	public List<RemusWork> getWorkQueue( RemusInstance job, int maxCount ) {
		List<RemusWork> out = new LinkedList<RemusWork>();
		for ( RemusApplet applet : members.values() ) {
			InstanceStatus status = applet.status;
			if ( status.instance.containsKey(job) ) {
				for ( Long jobID : status.instance.get(job).jobsRemaining ) {
					if ( out.size() < maxCount ) {
						out.add( new RemusWork(this, applet, job, jobID) );
					}
				}
			} else {
				if ( !applet.isComplete(job) && applet.isReady(job) ) {
					applet.status.addInstance(job);
					for ( Long jobID : status.instance.get(job).jobsRemaining ) {
						if ( out.size() < maxCount ) {
							out.add( new RemusWork(this, applet, job, jobID) );
						}
					}
				}
			}
		}
		return out;
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


	private void setInputs() {
		inputs = new HashMap<InputReference, RemusApplet>();
		for ( RemusApplet applet : members.values() ) {
			if ( applet.hasInputs() ) {
				for ( InputReference iref : applet.getInputs() ) {
					if ( iref.dynamicInput || !iref.isLocal() || !parent.containsKey( iref.getPath() )) {
						inputs.put(iref, applet);
					}
				}
			}
		}
	}

	public Collection<RemusApplet> getMembers() {
		return members.values();
	}

	public RemusApplet getApplet(String path) {
		return members.get(path);
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
			inputs.get(iref).status.addInstance( instance );
		}
		for ( RemusApplet applet : members.values() ) {
			if ( !applet.hasInputs() && applet instanceof SplitterApplet ) {
				applet.status.addInstance(instance);
			}
		}
		jobs.add(instance);
	}

	public CodeManager getCodeManager() {
		return parent;		
	}

}
