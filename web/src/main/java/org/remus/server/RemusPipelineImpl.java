package org.remus.server;

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

import org.apache.thrift.TException;
import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.WorkStatus;

import org.remus.manage.WorkStatusImpl;
import org.remus.serverNodes.AttachListView;
import org.remus.serverNodes.PipelineAgentView;
import org.remus.serverNodes.PipelineErrorView;
import org.remus.serverNodes.PipelineInstanceListViewer;
import org.remus.serverNodes.PipelineInstanceView;
import org.remus.serverNodes.PipelineListView;
import org.remus.serverNodes.PipelineStatusView;
import org.remus.serverNodes.ResetInstanceView;
import org.remus.serverNodes.SubmitView;
import org.remus.work.RemusAppletImpl;
import org.remus.work.Submission;
import org.remusNet.JSON;
import org.remusNet.KeyValPair;
import org.remusNet.RemusAttach;
import org.remusNet.RemusDB;
import org.remusNet.thrift.AppletRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemusPipelineImpl implements BaseNode, RemusPipeline {

	public static final String CODE_FIELD = "_code";

	HashMap<String,BaseNode> children;

	private HashMap<String,RemusAppletImpl> members;
	Map<String, RemusAppletImpl> inputs;
	String id;
	RemusDB datastore;
	RemusAttach attachStore;
	RemusApp app;

	private Logger logger;

	public RemusPipelineImpl(RemusApp app, String id, RemusDB datastore, RemusAttach attachStore) {

		logger = LoggerFactory.getLogger(RemusPipelineImpl.class);


		this.app = app;
		members = new HashMap<String,RemusAppletImpl>();
		children = new HashMap<String, BaseNode>();
		children.put("@pipeline", new PipelineListView(this) );
		children.put("@submit", new SubmitView(this) );
		children.put("@status", new PipelineStatusView(this) );
		children.put("@instance", new PipelineInstanceListViewer(this) );
		children.put("@agent", new PipelineAgentView(this) );

		children.put("@error", new PipelineErrorView(this) );

		children.put("@reset", new ResetInstanceView(this) );

		inputs = new HashMap<String, RemusAppletImpl >();
		this.id = id;
		this.datastore = datastore;
		this.attachStore = attachStore;
		//children.put("@attach", new AttachListView(this.attachStore, this, RemusInstance.STATIC_INSTANCE, null ) );
	}

	public RemusApp getApp() {
		return app;
	}

	public void addApplet(RemusAppletImpl applet) {		
		applet.setPipeline( this );
		inputs = null; //invalidate input list
		members.put(applet.getID(), applet);
		//children.put(applet.getID(), applet);
	}

	public Set<WorkStatus> getWorkQueue( ) {
		if ( inputs == null ) {
			setupInputs();
		}
		Set<WorkStatus> out = new HashSet<WorkStatus>();
		for ( RemusAppletImpl applet : members.values() ) {
			out.addAll( applet.getWorkList() );
		}
		return out;
	}

	private void setupInputs() {
		inputs = new HashMap<String, RemusAppletImpl>();
		for ( RemusAppletImpl applet : members.values() ) {
			if ( applet.hasInputs() ) {
				for ( String iref : applet.getInputs() ) {
					if ( iref.compareTo("?") != 0 ) {
						inputs.put(iref, applet);
					}
				}
			}
		}
	}

	public Collection<RemusAppletImpl> getMembers() {
		return members.values();
	}

	public RemusAppletImpl getApplet(String path) {
		return members.get(path);
	}

	public Set<String> getInputs() {
		if ( inputs == null ) {
			setupInputs();
		}
		return inputs.keySet();
	}
	/*
	public RemusApplet getInputApplet( RemusPath ref ) {
		if ( inputs == null ) {
			setupInputs();
		}
		return inputs.get(ref);
	}	
	 */

	public RemusInstance getInstance(String name) {
		try {
			AppletRef arSubmit = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");
			for ( Object subObject : getDataStore().get(arSubmit, name) ) {
				Map subMap = (Map) subObject;
				return new RemusInstance( (String)subMap.get( Submission.InstanceField ) );
			}
			AppletRef arInstance = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance");
			for ( Object instObject : getDataStore().get( arInstance, name) ) {			
				return  new RemusInstance( name );				
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}

	public String getSubKey(RemusInstance inst) {
		AppletRef arInstance = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance");
		try {
			for ( Object instObject : getDataStore().get( arInstance, inst.toString() ) ) {			
				return (String)instObject;
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public Map getSubmitData(String subKey) {
		AppletRef arSubmit = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");
		try {
			for ( Object subObject : getDataStore().get( arSubmit, subKey) ) {
				Map subMap = (Map) subObject;
				return subMap;
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}	

	public void deleteInstance(RemusInstance instance) {
		try { 
			logger.info( "Deleting Instance " + instance );
			for ( RemusAppletImpl applet : members.values() ) {
				applet.deleteInstance(instance);
			}
			AppletRef arInstance = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance");
			datastore.deleteValue( arInstance, instance.toString());
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public boolean isComplete(RemusInstance inst) {
		boolean done = true;
		for ( RemusAppletImpl applet : members.values() ) {
			if ( ! WorkStatusImpl.isComplete(applet, inst) )
				done = false;
		}
		return done;
	}

	public int appletCount() {
		return members.size();
	}

	public RemusDB getDataStore() {
		return datastore;
	}

	public boolean hasApplet(String appletPath) {
		return members.containsKey( appletPath ) ;
	}

	public String getID() {
		return id;
	}

	public RemusAttach getAttachStore() {
		return attachStore;
	}



	public Iterable<KeyValPair> getSubmits() {
		AppletRef arSubmit= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");
		return datastore.listKeyPairs( arSubmit );
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		//Deletions should be done through one of the sub-views, or in a parent view
		throw new FileNotFoundException();
	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os)
	throws FileNotFoundException {
		if ( name.length() != 0 ) {
			throw new FileNotFoundException();
		}
		AppletRef arSubmit= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");
		for ( KeyValPair kv : datastore.listKeyPairs(arSubmit) ) {
			Map out = new HashMap();
			out.put(kv.getKey(), kv.getValue() );
			try {
				os.write( JSON.dumps(out).getBytes() );
				os.write("\n".getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void doPut(String name, String workerID, InputStream is, OutputStream os) throws FileNotFoundException {
		System.err.println( "PUTTING:" + name );
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	class PipelineAttachment implements BaseNode {

		String fileName;
		PipelineAttachment(String fileName) {
			this.fileName = fileName;
		}

		@Override
		public void doDelete(String name, Map params, String workerID) throws FileNotFoundException { }

		@Override
		public void doGet(String name, Map params, String workerID,
				OutputStream os)
		throws FileNotFoundException {
			AppletRef arSubmit= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, null );

			InputStream fis = attachStore.readAttachement(arSubmit, null, fileName);
			if ( fis != null ) {
				byte [] buffer = new byte[1024];
				int len;
				try {
					while ( (len = fis.read(buffer)) >= 0 ) {
						os.write( buffer, 0, len );
					}
					os.close();
					fis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
			} else {
				throw new FileNotFoundException();
			}			
		}

		@Override
		public void doPut(String name, String workerID, InputStream is,
				OutputStream os) throws FileNotFoundException {}

		@Override
		public void doSubmit(String name, String workerID, InputStream is,
				OutputStream os) throws FileNotFoundException {}

		@Override
		public BaseNode getChild(String name) {
			return null;
		}

	}


	@Override
	public BaseNode getChild(String name) {
		if ( children.containsKey(name) )
			return children.get(name);

		AppletRef arSubmit= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");

		try {
			for ( Object subObject : datastore.get(arSubmit, name) ) {
				RemusInstance inst = new RemusInstance( (String)((Map)subObject).get( Submission.InstanceField ) );
				return new PipelineInstanceView( this, inst  );
			}
		} catch (TException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		AppletRef arInstance= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance");

		try {
			for ( Object subObject : datastore.get( arInstance, name) ) {
				RemusInstance inst = new RemusInstance( name );
				return new PipelineInstanceView( this, inst  );
			}
		} catch (TException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		AppletRef arBaseAttach= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, null );

		try { 
			if ( attachStore.hasAttachment( arBaseAttach, null, name ) ) {
				return new PipelineAttachment( name );
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}

	public RemusInstance handleSubmission(String key, Map value) {

		RemusInstance inst;

		if ( ((Map)value).containsKey( Submission.AppletField ) ) {
			List<String> aList = (List)((Map)value).get(Submission.AppletField);
			inst = setupInstance( key, (Map)value, aList );					
		} else {
			inst = setupInstance( key, (Map)value, new LinkedList() );	
		}					

		//only add the main submission/instance records if they don't already exist
		//we've already fired off the setupInstance requests to the applets, so if new applets are
		//to be instanced in an exisiting pipeline instance, they will be, but the original submisison 
		//will remain
		
		AppletRef arSubmit= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit" );

		
		try {
			if ( ! getDataStore().containsKey(arSubmit, key) ) {
				((Map)value).put(Submission.SubmitKeyField, key );	

				((Map)value).put(Submission.InstanceField, inst.toString());	
				getDataStore().add( arSubmit, 
						(Long)0L, 
						(Long)0L, 
						key,
						value );
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		AppletRef arInstance= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance" );

		try {
			getDataStore().add(arInstance,
					0L, 0L,
					inst.toString(),
					key);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
		return inst;
	}


	public RemusInstance setupInstance(String name, Map params, List<String> appletList) {
		logger.info("Init submission " + name );
		Set<RemusAppletImpl> activeSet = new HashSet<RemusAppletImpl>();
		RemusInstance inst = new RemusInstance();

		AppletRef arSubmit= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@subit" );

		try {
			for ( Object subObject : datastore.get( arSubmit, name) ) {
				inst = new RemusInstance( (String)((Map)subObject).get( Submission.InstanceField ) );
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		for (String sObj : appletList) {
			RemusAppletImpl applet = getApplet((String)sObj);
			if ( applet != null ) {
				activeSet.add(applet);
				try {
					applet.createInstance(name, params, inst);
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		boolean added = false;
		do {
			added = false;
			for ( RemusAppletImpl applet : getMembers() ) {
				if ( !activeSet.contains(applet) ) {
					/*
					if (applet.getMode() == RemusApplet.STORE) {
						if (applet.createInstance(name, params, inst)) {
							added = true;
						}
						activeSet.add(applet);
					} else {
					 */
					for ( String iRef : applet.getInputs() ) {
						if ( iRef.compareTo("?") != 0 ) {
							RemusAppletImpl srcApplet = getApplet( iRef );
							if (activeSet.contains(srcApplet) ) {
								try {
									if (applet.createInstance(name, params, inst)) {
										added = true;
									}
								} catch (TException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								activeSet.add(applet);
							}
						}
					}
					//}
				} 
			}
		} while (added);

		app.getWorkManager().jobScan();
		app.getWorkManager().workPoll();
		logger.info("submission " + name + " started as " + inst);
		return inst;		
	}



}
