package org.remus;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.remus.applet.RemusApplet;

public class RemusPipeline {

	boolean dynamic = false;
	private HashMap<String,RemusApplet> members;
	Map<InputReference, RemusApplet> inputs;
	CodeManager parent;

	public RemusPipeline(CodeManager parent) {
		this.parent = parent;
		members = new HashMap<String,RemusApplet>();
		inputs = new HashMap<InputReference, RemusApplet >();
	}

	public void addApplet(RemusApplet applet) {
		if ( applet.hasInputs() ) {
			for ( InputReference iref : applet.getInputs() ) {
				if ( iref.getInputType() == InputReference.DynamicInput ) {
					dynamic = true;
				}
			}
		}
		applet.setPipeline( this );
		inputs = null; //invalidate input list
		members.put(applet.getPath(), applet);
	}

	public List<WorkDescription> getWorkQueue(int maxCount) {
		if ( inputs == null ) {
			setupInputs();
		}
		List<WorkDescription> out = new LinkedList<WorkDescription>();
		for ( RemusApplet applet : members.values() ) {			
			for ( RemusInstance instance : applet.getInstanceList() ) {
				for ( WorkDescription work : applet.getWorkList( instance ) ) {
					if ( out.size() < maxCount ) {
						out.add( work );					
					}
				}
			}
		}
		return out;
	}


	private void setupInputs() {
		inputs = new HashMap<InputReference, RemusApplet>();
		for ( RemusApplet applet : members.values() ) {
			if ( applet.hasInputs() ) {
				for ( InputReference iref : applet.getInputs() ) {
					if ( iref.getInputType() == InputReference.DynamicInput 
							|| iref.getInputType() == InputReference.AppletInput 
							|| !parent.containsKey( iref.getPortPath() )) {
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
			setupInputs();
		}
		return inputs.keySet();
	}

	public RemusApplet getInputApplet( InputReference ref ) {
		if ( inputs == null ) {
			setupInputs();
		}
		return inputs.get(ref);
	}
	public void addInstance(RemusInstance instance) {
		if ( inputs == null ) {
			setupInputs();
		}		
		for ( RemusApplet applet : members.values() ) {
			applet.addInstance(instance);
		}
	}

	public CodeManager getCodeManager() {
		return parent;		
	}

	public boolean isComplete(RemusInstance inst) {
		boolean done = true;
		for ( RemusApplet applet : members.values() ) {
			if ( !applet.isComplete(inst) )
				done = false;
		}
		return done;
	}

	public int appletCount() {
		return members.size();
	}

}
