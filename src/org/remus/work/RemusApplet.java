package org.remus.work;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpstore.JsonSerializer;
import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.mpstore.Serializer;
import org.mpstore.AttachStore;
import org.remus.CodeFragment;
import org.remus.RemusPath;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.serverNodes.BaseNode;


public class RemusApplet implements BaseNode {

	public static RemusApplet newApplet( String id, CodeFragment code, int type ) {
		RemusApplet out = new RemusApplet();

		switch (type) {
		case MAPPER: {
			out.workGenerator = MapGenerator.class; //new MapperApplet();	
			break;
		}
		case REDUCER: {
			out.workGenerator = ReduceGenerator.class;	
			break;
		}
		case SPLITTER: {
			out.workValue = 100;
			out.workGenerator = SplitGenerator.class;	
			break;
		}
		case MERGER: {
			out.workGenerator = MergeGenerator.class;	
			break;
		}
		case MATCHER: {
			out.workGenerator = MatchGenerator.class;	
			break;
		}
		case PIPE: {
			out.workValue = 100;
			out.workGenerator = PipeGenerator.class;	
			break;
		}
		}
		if ( out != null ) {
			out.code = code;
			out.id = id;
			out.type = type;
			out.inputs = null;
			out.activeInstances = new LinkedList<RemusInstance>();			
		}
		return out;
	}

	public static final int MAPPER = 1;
	public static final int MERGER = 2;
	public static final int MATCHER = 3;
	public static final int SPLITTER = 4;
	public static final int REDUCER = 5;
	public static final int PIPE = 6;
	public static final int STORE = 7;
	public static final int ADAPTOR = 8;

	public static final String WORKDONE_OP = "/@workdone";	

	public static final int INIT_OP_CODE = 0;
	public static final int WORKDONE_OP_CODE = 1;

	public int workValue = 1;
	Class workGenerator = null;
	private String id;
	List<RemusPath> inputs = null, lInputs = null, rInputs = null;
	List<String> outputs = null;
	CodeFragment code;
	MPStore datastore;
	int type;
	protected RemusPipeline pipeline = null;
	LinkedList<RemusInstance> activeInstances;
	private AttachStore attachstore;


	public void addInput( RemusPath in ) {
		if ( inputs == null )
			inputs = new ArrayList<RemusPath>();
		inputs.add(in);
	}	

	public void addLeftInput( RemusPath in ) {
		if ( lInputs == null )
			lInputs = new LinkedList<RemusPath>();
		lInputs.add(in);
		addInput(in);
	}

	public void addRightInput( RemusPath in ) {
		if ( rInputs == null )
			rInputs = new LinkedList<RemusPath>();
		rInputs.add(in);
		addInput(in);
	}

	public void addOutput( String name ) {
		if ( outputs == null ) 
			outputs = new LinkedList<String>();
		outputs.add(name);
	}

	public RemusPath getInput() {
		return inputs.get(0);
	}

	public RemusPath getLeftInput() {
		return lInputs.get(0);
	}

	public RemusPath getRightInput() {
		return rInputs.get(0);
	}

	public CodeFragment getCode() {
		return code;
	}

	public List<RemusPath> getInputs() {
		if ( inputs != null )
			return inputs;
		return new ArrayList<RemusPath>();
	}

	public String [] getOutputs() {
		if ( outputs != null  )
			return outputs.toArray( new String[0] );
		return new String[0];
	}

	public String getPath() {
		return "/" + pipeline.getID() + "/" + id;
	}

	public String getSource() {
		return code.getSource();
	}

	/*
	public void setCodeType(String type) {
		codeType = type;
	}
	 */
	public void setPipeline(RemusPipeline remusPipeline) {
		this.pipeline = remusPipeline;		
		this.datastore = remusPipeline.getDataStore();
		this.attachstore = remusPipeline.getAttachStore();
	}

	public RemusPipeline getPipeline() {
		return this.pipeline;
	}


	public boolean isReady( RemusInstance remusInstance ) {
		if ( type==STORE )
			return false;
		if ( hasInputs() ) {
			boolean allReady = true;
			for ( RemusPath iRef : inputs ) {
				if ( iRef.getInputType() == RemusPath.AppletInput ) {
					RemusApplet iApplet = getPipeline().getApplet( iRef.getApplet() );
					if ( iApplet != null ) {
						if ( !iApplet.isComplete(remusInstance) ) {
							allReady = false;
						}
					} else {
						allReady = false;
					}
				} else if ( iRef.getInputType() == RemusPath.DynamicInput ) {
					/*
					if ( datastore.get( getPath() + "@submit", RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) == null ) {
						allReady = false;
					}
					 */
				} else if (  iRef.getInputType() == RemusPath.AttachInput ) {
				} else {				
					allReady = false;
				}
			}
			return allReady;
		}		
		return true;
	}


	public boolean isComplete( RemusInstance remusInstance ) {
		boolean found = false;
		for ( Object statObj : datastore.get( getPath() + "/@status", RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) ) {
			if ( statObj != null && ((Map)statObj).containsKey( WORKDONE_OP ) ) {
				found = true;
			}
		}
		if ( found ) {
			for ( String key : datastore.listKeys(  getPath() + "/@error", remusInstance.toString() ) ) {
				found = false;
			}
		}
		return found;
	}

	public boolean isInError(  RemusInstance remusInstance ) {
		boolean found = false;
		for ( String key : datastore.listKeys(  getPath() + "/@error", remusInstance.toString() ) ) {
			found = true;
		}
		return found;
	}

	public void setComplete(RemusInstance remusInstance) {
		Object statObj = null;
		for ( Object curObj : datastore.get( getPath() + "/@status", RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) ) {
			statObj = curObj;
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		((Map)statObj).put(WORKDONE_OP, true);
		datastore.add( getPath() + "/@status", RemusInstance.STATIC_INSTANCE_STR, 0, 0, remusInstance.toString(), statObj );
		datastore.delete( getPath() + "/@done", remusInstance.toString() );
	}

	public boolean hasInputs() {
		if ( inputs == null )
			return false;
		return true;
	}

	public int getType() {
		return type;
	}

	public Map<AppletInstance,Set<WorkKey>> getWorkList(int maxListSize) {
		HashMap<AppletInstance,Set<WorkKey>> out = new HashMap<AppletInstance,Set<WorkKey>>();		
		for ( RemusInstance inst : getActiveInstanceList() ) {
			if ( out.size() < maxListSize ) {
				if ( !isComplete(inst) ) {
					if ( isReady(inst)) {
						try {
							System.err.println("GENERATING WORK: " + getPath() + " " + inst.toString() );
							WorkGenerator gen = (WorkGenerator) workGenerator.newInstance();
							Set<WorkKey> workSet =  gen.getActiveKeys(this, inst, maxListSize - out.size());
							AppletInstance ai =  gen.getAppletInstance();
							assert ai != null;
							out.put( ai, workSet );
							if ( gen.isDone() ) {
								setComplete(inst);
							}
						} catch (InstantiationException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} catch (IllegalAccessException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}					
					}
				}
			}
		}
		return out;
	}

	public void finishWork(RemusInstance remusInstance, long jobID, String workerName, long emitCount) {
		datastore.add(getPath() + "/@done", remusInstance.toString(), 0L, 0L, Long.toString(jobID), workerName );
	}

	private void addInstance(RemusInstance instance, String srcPath) {
		if ( !datastore.containsKey(getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, instance.toString() ) ) {
			datastore.add(getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, INIT_OP_CODE, 0, instance.toString(), srcPath);
		}		
	}

	public Collection<RemusInstance> getInstanceList() {
		Collection<RemusInstance> out = new HashSet<RemusInstance>( );
		for ( String key : datastore.listKeys( getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR ) ) {
			out.add( new RemusInstance( key ) );
		}		
		for ( RemusPath iRef : getInputs() ) {
			if ( iRef.getInputType() == RemusPath.AppletInput ) {
				for ( String key : datastore.listKeys(iRef.getAppletPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR) ) {
					RemusInstance inst = new RemusInstance(key);
					if ( !out.contains( inst ) ) {
						addInstance(inst, iRef.getPath());
						out.add(inst);
					}
				}
			} 
		}
		/*
		for ( KeyValuePair kv : datastore.listKeyPairs(getPath() + "@submit", RemusInstance.STATIC_INSTANCE_STR) ) {
			RemusInstance inst = new RemusInstance((String)kv.getValue());
			if ( !out.contains( inst ) ) {
				addInstance(inst, kv.getKey());
				out.add(inst);
			}
		}*/		
		return out;
	}


	public Collection<RemusInstance> getActiveInstanceList() {
		Collection<RemusInstance> out = getInstanceList();
		Collection<RemusInstance> removeList = new HashSet<RemusInstance>();
		for ( RemusInstance inst : out ) {
			long timestamp = datastore.getTimeStamp(getPath() + "/@done", inst.toString());
			boolean invalid = false;
			for ( RemusPath iRef : getInputs() ) {
				if ( iRef.getInputType() == RemusPath.AppletInput ) {			
					long othertime = datastore.getTimeStamp(iRef.getAppletPath() + "/@done", inst.toString() );
					if ( othertime > timestamp )
						invalid = true;
				}				
			}
			if ( !invalid ) {
				boolean hasDone = false;
				for ( Object val : datastore.get(getPath() + "/@status", RemusInstance.STATIC_INSTANCE_STR, inst.toString() ) ) {
					if ( ((Map)val).containsKey( WORKDONE_OP ) )
						hasDone = true;
				}
				if ( hasDone )		
					removeList.add(inst);
			}
		}		
		out.removeAll(removeList);		
		return out;
	}


	public RemusInstance createInstance(String submitKey) {
		String instStr = null;
		for ( KeyValuePair kv : datastore.listKeyPairs( getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR ) ) {
			if ( submitKey.compareTo( (String)kv.getValue() ) == 0 ) {
				instStr = kv.getKey();
			}
		}
		RemusInstance inst;
		if ( instStr == null ) {
			inst = new RemusInstance();
			datastore.add(getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, 0L, 0L, inst.toString(), submitKey);
		} else {
			inst = new RemusInstance(instStr);
		}
		return inst;
	};

	public void deleteInstance(RemusInstance instance) {
		datastore.delete(getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, instance.toString() );		
		datastore.delete(getPath() + "/@status", RemusInstance.STATIC_INSTANCE_STR, instance.toString() );		
		//datastore.delete(getPath() + "@submit", RemusInstance.STATIC_INSTANCE_STR, instance.toString() );
		datastore.delete(getPath() + "/@done", instance.toString() );		
		datastore.delete(getPath() + "/@data", instance.toString() );		
		datastore.delete(getPath() + "/@error", instance.toString() );
		attachstore.delete(getPath() + "/@attach", instance.toString() );

		for ( String subname : getOutputs() ) {
			datastore.delete( getPath() + "." + subname + "/@data", instance.toString() );
		}		
		datastore.delete( getPath() + "/@done", RemusInstance.STATIC_INSTANCE_STR, instance.toString() );		
	}

	public void errorWork(RemusInstance inst, long jobID, String workerID, String error) {
		datastore.add( getPath() + "/@error", inst.toString(), 0L, 0L, Long.toString(jobID), error);
	}

	public void deleteErrors(RemusInstance inst) {
		datastore.delete(getPath() + "/@error", inst.toString() );		
	};

/*
	public Set<Integer> formatInput(RemusPath path, InputStream inputStream, Serializer serializer ) {
		Set<Integer> outSet = null;
		if ( type == STORE ) {
			if ( code.getLang().compareTo( "couchdb" ) == 0 ) {
				try {
					JsonSerializer json = new JsonSerializer();
					StringBuilder sb = new StringBuilder();
					byte [] buffer = new byte[1024];
					int len;
					while ((len = inputStream.read(buffer)) > 0) {
						sb.append( new String(buffer, 0, len ));
					}
					Object obj = json.loads(sb.toString());
					String key = (String) ((Map)obj).get( "_id" );
					datastore.add(getPath() + "/@data", path.getInstance(), 0, 0, key, obj);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if ( code.getLang().compareTo("json") == 0 ) {
				try {
					JsonSerializer json = new JsonSerializer();
					StringBuilder sb = new StringBuilder();
					byte [] buffer = new byte[1024];
					int len;
					while ((len = inputStream.read(buffer)) > 0) {
						sb.append( new String(buffer, 0, len ));
					}
					Map objMap = (Map)json.loads(sb.toString());
					for ( Object key :objMap.keySet() ) {
						datastore.add(getPath() + "/@data", path.getInstance(), 0, 0, (String)key, objMap.get(key));
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			outSet = new HashSet<Integer>();
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				String curline = null;
				List<KeyValuePair> inputList = new ArrayList<KeyValuePair>();
				while ( (curline = br.readLine() ) != null ) {
					Map inObj = (Map)serializer.loads(curline);	
					long jobID = Long.parseLong( inObj.get("id").toString() );
					outSet.add((int)jobID);
					inputList.add( new KeyValuePair( jobID, 
							(Long)inObj.get("order"), (String)inObj.get("key") , 
							inObj.get("value") ) );
				}
				datastore.add( path.getViewPath(), 
						path.getInstance(),
						inputList );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		return outSet;
	}
 */
	
	@Override
	public int hashCode() { 
		return getPath().hashCode();
	};

	@Override
	public boolean equals(Object obj) {
		RemusApplet a = (RemusApplet)obj;
		return a.getPath().equals(getPath());
	}

	public MPStore getDataStore() {
		return datastore;
	}

	public String getID() {
		return id;
	}

	public AttachStore getAttachStore() {
		return pipeline.getAttachStore();
	}

	public int getWorkValue() {
		return workValue;
	}

	/*
	public String getInstanceSrc(RemusInstance remusInstance) {
		String out = null;
		for ( Object obj : datastore.get( getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString()) ) {
			out = (String)obj;
		}
		return out;
	}
	
	public Object getStatus(RemusInstance remusInstance) {
		Object out = null;
		for ( Object obj : datastore.get( getPath() + "/@status", RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString()) ) {
			out = obj;
		}
		return out;	
	}

	
	public String getInstanceSubmit(RemusInstance remusInstance) {
		Object out = null;
		for ( Object obj : datastore.get( getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString()) ) {
			out = obj;
		}
		return (String)out;			
	}
 */
	
	@Override
	public void doDelete(Map params) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doGet(String name, Map params, String workerID, Serializer serial,
			OutputStream os) throws FileNotFoundException {
		System.err.println("APPLET_GETTING:" + name );	
		
		if ( name.length() == 0 ) {
			Map out = new HashMap();
			for ( KeyValuePair kv : getDataStore().listKeyPairs(getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR)) {
				out.put(kv.getKey(), kv.getValue() );
			}
			try {
				os.write( serial.dumps(out).getBytes() );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void doPut(String name, String workerID, Serializer serial, InputStream is, OutputStream os) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {
	
		
		
	}

	@Override
	public BaseNode getChild(String name) {
		
		if ( name.compareTo("@instance") == 0 ) {
			return new InstanceListView(this);
		}
		if ( name.compareTo("@error") == 0 ) {
			return new InstanceErrorView(this);
		}
		if ( name.compareTo("@status") == 0 ) {
			return new InstanceStatusView(this);
		}
		
		
		//if ( name.compareTo("@work") == 0 ) {
		//	return new WorkView(this);
		//}
		if ( datastore.containsKey( getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR,  name ) ) {
			return new AppletInstanceView( this, new RemusInstance( name ) );
		} else if ( datastore.containsKey( "/" + getPipeline().getID() + "/@submit", RemusInstance.STATIC_INSTANCE_STR,  name ) ) {
			for (Object obj : datastore.get(  "/" + getPipeline().getID() + "/@submit", RemusInstance.STATIC_INSTANCE_STR,  name ) ) {
				String instStr = (String)((Map)obj).get("_instance");
				if ( instStr != null ) {
					return new AppletInstanceView( this, new RemusInstance( instStr ) );
				}
			}
		}
		return null;
	}

}
