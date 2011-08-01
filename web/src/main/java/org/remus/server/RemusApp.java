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

import org.apache.thrift.TException;
import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.manage.WorkManagerImpl;
import org.remus.serverNodes.ManageApp;
import org.remus.serverNodes.ServerStatusView;
import org.remus.serverNodes.StoreInfoView;
import org.remus.work.RemusAppletImpl;
import org.remusNet.ConnectionException;
import org.remusNet.JSON;
import org.remusNet.KeyValPair;
import org.remusNet.RemusAttach;
import org.remusNet.RemusDB;
import org.remusNet.thrift.AppletRef;

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
	RemusDB rootStore;
	RemusAttach rootAttachStore;
	private WorkManagerImpl workManage;

	public RemusApp( Map params ) throws RemusDatabaseException {
		this.params = params;
		//scanSource(srcbase);
		try {
			loadPipelines();		
		} catch (TException e ) {
			throw new RemusDatabaseException( e.toString() );
		}
	}


	public void loadPipelines() throws TException {
		try { 
			children = new HashMap<String,BaseNode>();
			children.put("@pipeline", new PipelineView(this) );
			children.put("@status", new ServerStatusView(this) );
			children.put("@manage", new ManageApp() );

			children.put("@db", new StoreInfoView( this ) );

			pipelines = new HashMap<String, RemusPipelineImpl>();
			String mpStore = (String)params.get(RemusApp.CONFIG_STORE);
			Class<RemusDB> mpClass = (Class<RemusDB>) Class.forName(mpStore);			
			rootStore = (RemusDB) mpClass.newInstance();

			try {
				rootStore.init(params);			
			} catch (ConnectionException e) {
				throw new TException(e);
			}
			String attachStoreName = (String)params.get(RemusApp.CONFIG_ATTACH_STORE);
			Class<RemusAttach> attachClass = (Class<RemusAttach>)Class.forName(attachStoreName);			
			rootAttachStore = (RemusAttach) attachClass.newInstance();
			rootAttachStore.init(params);
			AppletRef arPipe = new AppletRef( null, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" );
			for ( KeyValPair kv : rootStore.listKeyPairs( arPipe ) ) {
				loadPipeline(kv.getKey(), rootStore, rootAttachStore);
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
		} 
		workManage = new WorkManagerImpl(this);
		children.put("@work", workManage);
	}

	public void loadPipeline(String name, RemusDB store,
			RemusAttach attachStore) throws TException {
		RemusPipelineImpl pipeline = new RemusPipelineImpl(this, name, store, attachStore);		
		AppletRef arPipeline = new AppletRef( name, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" );
		for ( KeyValPair kv : store.listKeyPairs( arPipeline ) ) {
			List<RemusAppletImpl> applets = loadApplet( name, kv.getKey(), store );
			for ( RemusAppletImpl applet : applets ) {
				pipeline.addApplet(applet);
			}
		}
		pipelines.put(pipeline.id, pipeline);	
		children.put(pipeline.id, pipeline );
	}


	private List<RemusAppletImpl> loadApplet(String pipelineName, String name, RemusDB store ) throws TException {

		List<RemusAppletImpl> out = new LinkedList<RemusAppletImpl>();

		AppletRef arPipeline = new AppletRef( pipelineName, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" );
		Map appletObj = null;
		for ( Object obj : store.get( arPipeline, name) ) {
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



	public void deleteApplet(RemusPipelineImpl pipeline, RemusAppletImpl applet) throws TException {		
		for ( RemusInstance inst : applet.getInstanceList() ) {
			applet.deleteInstance(inst);
		}
		String dbPath = "/" + pipeline.getID() + "/@pipeline";
		AppletRef arPipeline = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() );
		rootStore.deleteStack( arPipeline );
		loadPipelines();
	}

	public void deletePipeline(RemusPipelineImpl pipe) throws TException {
		for ( RemusAppletImpl applet : pipe.getMembers() ) {
			deleteApplet(pipe, applet);
		}
		rootStore.deleteValue( new AppletRef( null, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" ), pipe.getID() );
		rootStore.deleteStack( new AppletRef( pipe.id, RemusInstance.STATIC_INSTANCE_STR, "/@submit" ) );
		rootStore.deleteStack( new AppletRef( pipe.id, RemusInstance.STATIC_INSTANCE_STR, "/@instance" ) );		
		loadPipelines();
	}

	public void putPipeline(String name, Object data) throws TException {
		AppletRef ar = new AppletRef( null, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" );
		rootStore.add(ar, 0L, 0L, name, data );		
		loadPipelines();
	}

	public void putApplet(RemusPipelineImpl pipe, String name, Object data) throws TException { 
		AppletRef ar = new AppletRef( pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" );
		rootStore.add( ar, 0L, 0L, name, data );
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

	public RemusDB getRootDatastore() {
		return rootStore;
	}

	public RemusAttach getRootAttachStore() {
		return rootAttachStore;
	}


	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		throw new FileNotFoundException(name);
	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os) throws FileNotFoundException {

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
			os.write( JSON.dumps(out).getBytes() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void doPut(String name, String workerID, InputStream is, OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException(name);
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
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


	public void passCall( int type, String path, Map parameterMap, String workerID, InputStream inputStream, OutputStream outputStream) throws FileNotFoundException {
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
					curNode.doGet( sb.toString(), parameterMap, workerID, outputStream );
				if ( type == PUT_CALL )
					curNode.doPut( sb.toString(), workerID, inputStream, outputStream );
				if ( type == SUBMIT_CALL )
					curNode.doSubmit( sb.toString(), workerID, inputStream, outputStream );
				if ( type == DELETE_CALL )
					curNode.doDelete( sb.toString(), parameterMap, workerID );
				called = true;
			}
		}
		if ( !called ) {
			if ( type == GET_CALL )
				curNode.doGet( "", parameterMap, workerID, outputStream );
			if ( type == PUT_CALL )
				curNode.doPut( "", workerID, inputStream, outputStream );
			if ( type == SUBMIT_CALL )
				curNode.doSubmit( "", workerID, inputStream, outputStream );
			if ( type == DELETE_CALL )
				curNode.doDelete( "", parameterMap, workerID );
		}
	}

	public Map getParams() {
		return params;
	}




}




