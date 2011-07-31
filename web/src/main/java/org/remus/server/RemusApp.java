package org.remus.server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.mpstore.AttachStore;
import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.mpstore.MPStoreConnectException;
import org.mpstore.Serializer;
import org.mpstore.impl.JsonSerializer;
import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.manage.WorkManagerImpl;
import org.remus.serverNodes.ManageApp;
import org.remus.serverNodes.ServerStatusView;
import org.remus.work.RemusAppletImpl;
import org.remus.work.StoreInfoView;

/**
 * Main Interface to Remus applications. In charge of root database interface and pipeline 
 * management.
 * 
 * @author kellrott
 *
 */

public class RemusApp implements BaseNode {
	/**
	 * Class name for MPstore interface.
	 * @see org.mpstore.impl.ThriftStore
	 */
	public static final String CONFIG_STORE = "org.remus.mpstore";
	public static final String CONFIG_WORK = "org.remus.workdir";
	public static final String CONFIG_ATTACH_STORE = "org.remus.attachstore";

	Map<String,RemusPipelineImpl> pipelines;
	Map<String,BaseNode> children;

	Map params;
	MPStore rootStore;
	AttachStore rootAttachStore;
	private WorkManagerImpl workManage;

	public RemusApp( Map params ) throws RemusDatabaseException {
		this.params = params;
		//scanSource(srcbase);
		loadPipelines();		
	}


	public void loadPipelines() throws RemusDatabaseException {
		try { 
			children = new HashMap<String,BaseNode>();
			children.put("@pipeline", new PipelineView(this) );
			children.put("@status", new ServerStatusView(this) );
			children.put("@manage", new ManageApp() );
			
			children.put("@db", new StoreInfoView( this ) );
			
			pipelines = new HashMap<String, RemusPipelineImpl>();
			String mpStore = (String)params.get(RemusApp.CONFIG_STORE);
			Class<?> mpClass = Class.forName(mpStore);			
			rootStore = (MPStore) mpClass.newInstance();
			Serializer serializer = new JsonSerializer();
			rootStore.initMPStore(serializer, params);			

			String attachStoreName = (String)params.get(RemusApp.CONFIG_ATTACH_STORE);
			Class<?> attachClass = Class.forName(attachStoreName);			
			rootAttachStore = (AttachStore) attachClass.newInstance();
			rootAttachStore.initAttachStore(params);
			for ( KeyValuePair kv : rootStore.listKeyPairs("/@pipeline", RemusInstance.STATIC_INSTANCE_STR ) ) {
				loadPipeline(kv.getKey(), rootStore, serializer, rootAttachStore);
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MPStoreConnectException e) {
			throw new RemusDatabaseException(e.toString());
		} 
		workManage = new WorkManagerImpl(this);
		children.put("@work", workManage);
	}

	public void loadPipeline(String name, MPStore store,
			Serializer serializer, AttachStore attachStore) {
		RemusPipelineImpl pipeline = new RemusPipelineImpl(this, name, store, attachStore);		
		for ( KeyValuePair kv : store.listKeyPairs( "/" + name + "/@pipeline", RemusInstance.STATIC_INSTANCE_STR) ) {
			List<RemusAppletImpl> applets = loadApplet( name, kv.getKey(), store, serializer );
			for ( RemusAppletImpl applet : applets ) {
				pipeline.addApplet(applet);
			}
		}
		pipelines.put(pipeline.id, pipeline);	
		children.put(pipeline.id, pipeline );
	}


	private List<RemusAppletImpl> loadApplet(String pipelineName, String name, MPStore store, Serializer serializer) {

		List<RemusAppletImpl> out = new LinkedList<RemusAppletImpl>();

		String dbPath = "/" + pipelineName + "/@pipeline";

		Map appletObj = null;
		for ( Object obj : store.get(dbPath, RemusInstance.STATIC_INSTANCE_STR, name) ) {
			appletObj = (Map)obj;		
		}

		String mode = (String)appletObj.get( RemusAppletImpl.MODE_FIELD );
		String codeType = (String)appletObj.get( RemusAppletImpl.TYPE_FIELD);

		Integer appletType = null;
		if (mode.compareTo("map") == 0) {
			appletType = RemusAppletImpl.MAPPER;
		}
		if (mode.compareTo("reduce") == 0) {
			appletType = RemusAppletImpl.REDUCER;
		}
		if ( mode.compareTo("pipe") == 0 ) {
			appletType = RemusAppletImpl.PIPE;
		}
		if ( mode.compareTo("merge") == 0 ) {
			appletType = RemusAppletImpl.MERGER;
		}
		if ( mode.compareTo("match") == 0 ) {
			appletType = RemusAppletImpl.MATCHER;
		}
		if ( mode.compareTo("split") == 0 ) {
			appletType = RemusAppletImpl.SPLITTER;
		}
		if ( mode.compareTo("store") == 0 ) {
			appletType = RemusAppletImpl.STORE;
		}
		if ( mode.compareTo("agent") == 0 ) {
			appletType = RemusAppletImpl.AGENT;
		}
		if (appletType == null)
			return null;
		RemusAppletImpl applet = RemusAppletImpl.newApplet(name, codeType, appletType);

		if ( appletType == RemusAppletImpl.MATCHER || appletType == RemusAppletImpl.MERGER ) {
			//try {
			String lInput = (String) appletObj.get( RemusAppletImpl.LEFT_SRC );
			//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
			applet.addLeftInput( lInput );
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
			//try {
			String rInput = (String) appletObj.get( RemusAppletImpl.RIGHT_SRC );
			//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
			applet.addRightInput(rInput);
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
		} else {
			//try {
			Object src = appletObj.get( RemusAppletImpl.SRC );
			
			if ( src instanceof String ) {
				String input = (String) src;
				//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
				applet.addInput(input);
			}
			if ( src instanceof List ) {
				for ( Object obj : (List)  src )	{
					//RemusPath path = new RemusPath( this, (String)obj, pipelineName, name );
					applet.addInput((String)obj);
				}
			}
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
		}
		if ( appletObj.containsKey( RemusAppletImpl.OUTPUT_FIELD ) ) {
			for ( Object nameObj : (List)appletObj.get(  RemusAppletImpl.OUTPUT_FIELD  ) ) {
				RemusAppletImpl outApplet = RemusAppletImpl.newApplet(name + "." + (String)nameObj, null, RemusAppletImpl.OUTPUT );
				for (String input : applet.getInputs() ) {
					outApplet.addInput(input);
				}
				
				out.add(outApplet);
			}
		}		
		out.add(applet);
		return out;
	}



	public void deleteApplet(RemusPipelineImpl pipeline, RemusAppletImpl applet) throws RemusDatabaseException {		
		for ( RemusInstance inst : applet.getInstanceList() ) {
			applet.deleteInstance(inst);
		}
		String dbPath = "/" + pipeline.getID() + "/@pipeline";
		rootStore.delete(dbPath, RemusInstance.STATIC_INSTANCE_STR, applet.getID() );
		loadPipelines();
	}

	public void deletePipeline(RemusPipelineImpl pipe) throws RemusDatabaseException {
		for ( RemusAppletImpl applet : pipe.getMembers() ) {
			deleteApplet(pipe, applet);
		}
		rootStore.delete("/@pipeline", RemusInstance.STATIC_INSTANCE_STR, pipe.getID() );
		rootStore.delete( "/" + pipe.id + "/@submit", RemusInstance.STATIC_INSTANCE_STR  );
		rootStore.delete( "/" + pipe.id + "/@instance", RemusInstance.STATIC_INSTANCE_STR  );

		loadPipelines();
	}

	public void putPipeline(String name, Object data) throws RemusDatabaseException {
		rootStore.add("/@pipeline", RemusInstance.STATIC_INSTANCE_STR, 0L, 0L, name, data );		
		loadPipelines();
	}

	public void putApplet(RemusPipelineImpl pipe, String name, Object data) throws RemusDatabaseException { 
		rootStore.add("/" + pipe.getID() + "/@pipeline", RemusInstance.STATIC_INSTANCE_STR, 0L, 0L, name, data );
		loadPipelines();
	}	

	public Map<String, PluginConfig> getPluginMap() {
		// TODO Auto-generated method stub
		return null;
	}


	
	
	public WorkManagerImpl getWorkManager() {
		return workManage;
	}
	
	public RemusPipelineImpl getPipeline( String name ) {
		return pipelines.get(name);
	}

	public Collection<RemusPipelineImpl> getPipelines() {
		return pipelines.values();
	}
	public RemusAppletImpl getApplet(String appletPath) {
		if (appletPath.startsWith("/"))
			appletPath = appletPath.replaceFirst("^\\/", "");
		String []tmp = appletPath.split("/");
		if ( pipelines.containsKey(tmp[0]) )
			return pipelines.get(tmp[0]).getApplet(tmp[1]);
		return null;
	}

	public MPStore getRootDatastore() {
		return rootStore;
	}

	public AttachStore getRootAttachStore() {
		return rootAttachStore;
	}
	

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		throw new FileNotFoundException(name);
	}

	@Override
	public void doGet(String name, Map params, String workerID, Serializer serial, OutputStream os) throws FileNotFoundException {

		if ( name.length() != 0 ) {
			throw new FileNotFoundException(name);
		}

		Map out = new HashMap();
		List<String> oList = new ArrayList<String>();
		for ( Object pipeObj : getPipelines() ) {
			oList.add( ((RemusPipelineImpl)pipeObj).id );
		}
		out.put("@", oList);
		try {
			os.write( serial.dumps(out).getBytes() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void doPut(String name, String workerID, Serializer serial, InputStream is, OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException(name);
	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException(name);		
	}

	@Override
	public BaseNode getChild(String name) {
		return children.get(name);
	}

	public static final int GET_CALL = 1;
	public static final int DELETE_CALL = 2;
	public static final int PUT_CALL = 3;
	public static final int SUBMIT_CALL = 4;


	public void passCall( int type, String path, Map parameterMap, String workerID, Serializer serial, InputStream inputStream, OutputStream outputStream) throws FileNotFoundException {
		String [] tmp = path.split("/");

		BaseNode curNode = this;
		Boolean called = false;
		for ( int i = 1; i < tmp.length && !called; i++ ) {		
			BaseNode next = curNode.getChild( tmp[i] );
			if ( next != null ) {
				curNode = next;
			} else {
				StringBuilder sb = new StringBuilder();
				for ( int j = i; j < tmp.length; j++) {
					if ( j != i )
						sb.append("/");
					sb.append( tmp[j] );
				}
				//System.err.println( curNode + " " + sb.toString() );
				if ( type == GET_CALL )
					curNode.doGet( sb.toString(), parameterMap, workerID, serial, outputStream );
				if ( type == PUT_CALL )
					curNode.doPut( sb.toString(), workerID, serial, inputStream, outputStream );
				if ( type == SUBMIT_CALL )
					curNode.doSubmit( sb.toString(), workerID, serial, inputStream, outputStream );
				if ( type == DELETE_CALL )
					curNode.doDelete( sb.toString(), parameterMap, workerID );
				called = true;
			}
		}
		if ( !called ) {
			if ( type == GET_CALL )
				curNode.doGet( "", parameterMap, workerID, serial, outputStream );
			if ( type == PUT_CALL )
				curNode.doPut( "", workerID, serial, inputStream, outputStream );
			if ( type == SUBMIT_CALL )
				curNode.doSubmit( "", workerID, serial, inputStream, outputStream );
			if ( type == DELETE_CALL )
				curNode.doDelete( "", parameterMap, workerID );
		}
	}

	public Map getParams() {
		return params;
	}

	


}




