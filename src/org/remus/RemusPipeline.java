package org.remus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpstore.AttachStore;
import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.mpstore.Serializer;
import org.remus.serverNodes.BaseNode;
import org.remus.work.AppletInstance;
import org.remus.work.RemusApplet;
import org.remus.work.Submission;
import org.remus.work.WorkKey;

public class RemusPipeline implements BaseNode {

	HashMap<String,BaseNode> children;

	private HashMap<String,RemusApplet> members;
	Map<RemusPath, RemusApplet> inputs;
	String id;
	MPStore datastore;
	AttachStore attachStore;
	RemusApp app;
	public RemusPipeline(RemusApp app, String id, MPStore datastore, AttachStore attachStore) {
		this.app = app;
		members = new HashMap<String,RemusApplet>();
		children = new HashMap<String, BaseNode>();
		children.put("@pipeline", new PipelineListView(this) );
		children.put("@submit", new SubmitView(this) );
		children.put("@status", new PipelineStatusView(this) );
		children.put("@instance", new PipelineInstanceListViewer(this) );

		inputs = new HashMap<RemusPath, RemusApplet >();
		this.id = id;
		this.datastore = datastore;
		this.attachStore = attachStore;
		children.put("@attach", new AttachListView(this.attachStore, "/" + id + "/@attach", RemusInstance.STATIC_INSTANCE_STR, null ) );
	}

	public RemusApp getApp() {
		return app;
	}

	public void addApplet(RemusApplet applet) {		
		applet.setPipeline( this );
		inputs = null; //invalidate input list
		members.put(applet.getID(), applet);
		//children.put(applet.getID(), applet);
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

		datastore.delete("/" + getID() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, instance.toString());

		String submitKey = null;
		for ( KeyValuePair kv : datastore.listKeyPairs("/" + getID() + "/@submit", RemusInstance.STATIC_INSTANCE_STR)) {
			String subinst = (String)((Map)kv.getValue()).get(Submission.InstanceField);
			if ( subinst != null && subinst.compareTo(instance.toString()) == 0 ) {
				submitKey = kv.getKey();
			}
		}
		if ( submitKey != null ) {
			datastore.delete("/" + getID() + "/@submit", RemusInstance.STATIC_INSTANCE_STR, submitKey );
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

	public AttachStore getAttachStore() {
		return attachStore;
	}



	public Iterable<KeyValuePair> getSubmits() {
		return datastore.listKeyPairs( "/" + getID() + "/@submit", 
				RemusInstance.STATIC_INSTANCE_STR );
	}

	@Override
	public void doDelete(String name, Map params, String workerID) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID, Serializer serial, OutputStream os)
	throws FileNotFoundException {


		for ( KeyValuePair kv : datastore.listKeyPairs( "/" + getID() + "/@submit", RemusInstance.STATIC_INSTANCE_STR ) ) {
			Map out = new HashMap();
			out.put(kv.getKey(), kv.getValue() );
			try {
				os.write( serial.dumps(out).getBytes() );
				os.write("\n".getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void doPut(String name, String workerID, Serializer serial, InputStream is, OutputStream os) {
		System.err.println( "PUTTING:" + name );
	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {
		// TODO Auto-generated method stub

	}
	@Override
	public BaseNode getChild(String name) {
		if ( children.containsKey(name) )
			return children.get(name);

		for ( Object subObject : datastore.get( "/" + getID() + "/@submit", RemusInstance.STATIC_INSTANCE_STR, name) ) {
			RemusInstance inst = new RemusInstance( (String)((Map)subObject).get( Submission.InstanceField ) );
			return new PipelineInstanceView( this, inst  );
		}


		for ( Object subObject : datastore.get( "/" + getID() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, name) ) {
			RemusInstance inst = new RemusInstance( name );
			return new PipelineInstanceView( this, inst  );
		}
		return null;
	}

	public RemusInstance setupInstance(String name, Map params, List<String> appletList) {
		Set<RemusApplet> activeSet = new HashSet<RemusApplet>();
		RemusInstance inst = new RemusInstance();
		for (String sObj : appletList) {
			RemusApplet applet = getApplet((String)sObj);
			if ( applet != null ) {
				activeSet.add(applet);
				applet.createInstance(name, params, inst);
			}
		}

		boolean added = false;
		do {
			added = false;
			for ( RemusApplet applet : getMembers() ) {
				if ( !activeSet.contains(applet) ) {
					if ( applet.getType() == RemusApplet.STORE) {
						if ( applet.createInstance(name, params, inst) ) 
							added = true;
						activeSet.add(applet);
					} else {
						for ( RemusPath iRef : applet.getInputs() ) {
							if ( iRef.getInputType() == RemusPath.AppletInput ) {
								RemusApplet srcApplet = getApplet(iRef.getApplet());
								if (activeSet.contains(srcApplet) ) {
									if ( applet.createInstance(name, params, inst) ) 
										added = true;
									activeSet.add(applet);
								}
							}
						}
					}
				} 
			}
		} while (added);
		return inst;		
	}


}
