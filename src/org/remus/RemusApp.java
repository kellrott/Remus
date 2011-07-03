package org.remus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpstore.AttachStore;
import org.mpstore.JsonSerializer;
import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.mpstore.MPStoreConnectException;
import org.mpstore.Serializer;
import org.remus.manage.WorkStatus;
import org.remus.manage.WorkManager;
import org.remus.serverNodes.BaseNode;
import org.remus.serverNodes.ManageApp;
import org.remus.serverNodes.ServerStatusView;
import org.remus.work.RemusApplet;
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
	 * @see org.mpstore.ThriftStore
	 */
	public static final String configStore = "org.remus.mpstore";
	public static final String configWork = "org.remus.workdir";
	public static final String configAttachStore = "org.remus.attachstore";

	//File srcbase;
	public String baseURL = "";
	Map<String,RemusPipeline> pipelines;
	Map<String,BaseNode> children;

	Map params;
	MPStore rootStore;
	AttachStore rootAttachStore;
	private WorkManager workManage;

	public RemusApp( Map params ) throws RemusDatabaseException {
		this.params = params;
		//scanSource(srcbase);
		loadPipelines();		
	}

	public void setBaseURL(String baseURL) {
		this.baseURL = baseURL;
	}

	public void loadPipelines() throws RemusDatabaseException {
		try { 
			children = new HashMap<String,BaseNode>();
			children.put("@pipeline", new PipelineView(this) );
			children.put("@status", new ServerStatusView(this) );
			children.put("@manage", new ManageApp() );
			
			children.put("@db", new StoreInfoView( this ) );
			
			pipelines = new HashMap<String, RemusPipeline>();
			String mpStore = (String)params.get(RemusApp.configStore);
			Class<?> mpClass = Class.forName(mpStore);			
			rootStore = (MPStore) mpClass.newInstance();
			Serializer serializer = new JsonSerializer();
			rootStore.initMPStore(serializer, params);			

			String attachStoreName = (String)params.get(RemusApp.configAttachStore);
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
			throw new RemusDatabaseException( e.toString() );
		} 
		workManage = new WorkManager(this);
		children.put("@work", workManage );
	}

	public void loadPipeline(String name, MPStore store, Serializer serializer, AttachStore attachStore) {
		RemusPipeline pipeline = new RemusPipeline(this, name, store, attachStore);		
		for ( KeyValuePair kv : store.listKeyPairs( "/" + name + "/@pipeline", RemusInstance.STATIC_INSTANCE_STR) ) {
			List<RemusApplet> applets = loadApplet( name, kv.getKey(), store, serializer );
			for ( RemusApplet applet : applets ) {
				pipeline.addApplet(applet);
			}
		}
		pipelines.put(pipeline.id, pipeline);	
		children.put(pipeline.id, pipeline );
	}


	private List<RemusApplet> loadApplet(String pipelineName, String name, MPStore store, Serializer serializer) {

		List<RemusApplet> out = new LinkedList<RemusApplet>();

		String dbPath = "/" + pipelineName + "/@pipeline";

		Map appletObj = null;
		for ( Object obj : store.get(dbPath, RemusInstance.STATIC_INSTANCE_STR, name) ) {
			appletObj = (Map)obj;		
		}

		String code = (String)appletObj.get( RemusApplet.CODE_FIELD );
		String type = (String)appletObj.get( RemusApplet.MODE_FIELD );
		String codeType = (String)appletObj.get( RemusApplet.TYPE_FIELD);

		CodeFragment cf =  new CodeFragment(codeType, code);
		Integer appletType = null;
		if ( type.compareTo("map") == 0 ) {
			appletType = RemusApplet.MAPPER;
		}
		if ( type.compareTo("reduce") == 0 ) {
			appletType = RemusApplet.REDUCER;
		}
		if ( type.compareTo("pipe") == 0 ) {
			appletType = RemusApplet.PIPE;
		}
		if ( type.compareTo("merge") == 0 ) {
			appletType = RemusApplet.MERGER;
		}
		if ( type.compareTo("match") == 0 ) {
			appletType = RemusApplet.MATCHER;
		}
		if ( type.compareTo("split") == 0 ) {
			appletType = RemusApplet.SPLITTER;
		}
		if ( type.compareTo("store") == 0 ) {
			appletType = RemusApplet.STORE;
		}
		if ( type.compareTo("agent") == 0 ) {
			appletType = RemusApplet.AGENT;
		}
		if (appletType == null)
			return null;
		RemusApplet applet = RemusApplet.newApplet(name, cf, appletType);

		if ( appletType == RemusApplet.MATCHER || appletType == RemusApplet.MERGER ) {
			//try {
			String lInput = (String) appletObj.get( RemusApplet.LEFT_SRC );
			//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
			applet.addLeftInput( lInput );
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
			//try {
			String rInput = (String) appletObj.get( RemusApplet.RIGHT_SRC );
			//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
			applet.addRightInput(rInput);
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
		} else {
			//try {
			if ( appletObj.get( RemusApplet.SRC ) instanceof String ) {
				String input = (String) appletObj.get(RemusApplet.SRC );
				//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
				applet.addInput(input);
			}
			if ( appletObj.get( RemusApplet.SRC ) instanceof List ) {
				for ( Object obj : (List)  appletObj.get( RemusApplet.SRC ) )	{
					//RemusPath path = new RemusPath( this, (String)obj, pipelineName, name );
					applet.addInput((String)obj);
				}
			}
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
		}
		if ( appletObj.containsKey( RemusApplet.OUTPUT_FIELD ) ) {
			for ( Object nameObj : (List)appletObj.get(  RemusApplet.OUTPUT_FIELD  ) ) {
				RemusApplet outApplet = RemusApplet.newApplet(name + "." + (String)nameObj, null, RemusApplet.OUTPUT );
				for (String input : applet.getInputs() ) {
					outApplet.addInput(input);
				}
				
				out.add(outApplet);
			}
		}		
		out.add(applet);
		return out;
	}



	public void deleteApplet(RemusPipeline pipeline, RemusApplet applet) throws RemusDatabaseException {		
		for ( RemusInstance inst : applet.getInstanceList() ) {
			applet.deleteInstance(inst);
		}
		String dbPath = "/" + pipeline.getID() + "/@pipeline";
		rootStore.delete(dbPath, RemusInstance.STATIC_INSTANCE_STR, applet.getID() );
		loadPipelines();
	}

	public void deletePipeline(RemusPipeline pipe) throws RemusDatabaseException {
		for ( RemusApplet applet : pipe.getMembers() ) {
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

	public void putApplet(RemusPipeline pipe, String name, Object data) throws RemusDatabaseException { 
		rootStore.add("/" + pipe.getID() + "/@pipeline", RemusInstance.STATIC_INSTANCE_STR, 0L, 0L, name, data );
		loadPipelines();
	}	

	public Map<String, PluginConfig> getPluginMap() {
		// TODO Auto-generated method stub
		return null;
	}


	
	
	public WorkManager getWorkManager() {
		return workManage;
	}
	
	public RemusPipeline getPipeline( String name ) {
		return pipelines.get(name);
	}

	public Collection<RemusPipeline> getPipelines() {
		return pipelines.values();
	}
	public RemusApplet getApplet(String appletPath) {
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
			oList.add( ((RemusPipeline)pipeObj).id );
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




