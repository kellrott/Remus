package org.remus;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.mpstore.MPStore;
import org.remus.work.AppletInstance;
import org.remus.work.RemusApplet;
import org.remus.work.WorkKey;

public class RemusPipeline {

	private HashMap<String,RemusApplet> members;
	Map<RemusPath, RemusApplet> inputs;
	String id;
	MPStore datastore;
	public RemusPipeline(String id, MPStore datastore) {
		members = new HashMap<String,RemusApplet>();
		inputs = new HashMap<RemusPath, RemusApplet >();
		this.id = id;
		this.datastore = datastore;
	}

	public void addApplet(RemusApplet applet) {		
		applet.setPipeline( this );
		inputs = null; //invalidate input list
		members.put(applet.getID(), applet);
	}

	public Map<AppletInstance,Set<WorkKey>> getWorkQueue(int maxCount) {
		if ( inputs == null ) {
			setupInputs();
		}
		Map<AppletInstance,Set<WorkKey>> out = new HashMap<AppletInstance,Set<WorkKey>>();
		for ( RemusApplet applet : members.values() ) {
			if ( out.size() < maxCount ) {
				out.putAll( applet.getWorkList( maxCount - out.size()) );
			}
		}
		return out;
	}

	private void setupInputs() {
		inputs = new HashMap<RemusPath, RemusApplet>();
		for ( RemusApplet applet : members.values() ) {
			if ( applet.hasInputs() ) {
				for ( RemusPath iref : applet.getInputs() ) {
					if ( iref.getInputType() == RemusPath.DynamicInput 
							|| iref.getInputType() == RemusPath.AppletInput 
							) { //|| !parent.containsKey( iref.getPortPath() )) {
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

	public Set<RemusPath> getInputs() {
		if ( inputs == null ) {
			setupInputs();
		}
		return inputs.keySet();
	}

	public RemusApplet getInputApplet( RemusPath ref ) {
		if ( inputs == null ) {
			setupInputs();
		}
		return inputs.get(ref);
	}	

	public void deleteInstance(RemusInstance instance) {
		for ( RemusApplet applet : members.values() ) {
			applet.deleteInstance(instance);
		}

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

	public MPStore getDataStore() {
		return datastore;
	}

	public boolean hasApplet(String appletPath) {
		return members.containsKey( appletPath ) ;
	}

	public String getID() {
		return id;
	}

}
