package org.remus.work;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpstore.AttachStore;
import org.mpstore.MPStore;
import org.remus.CodeFragment;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.serverNodes.AppletInstanceStatusView;


public class RemusApplet {

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
			out.workGenerator = PipeGenerator.class;	
			break;
		}
		case AGENT: {
			out.workGenerator = AgentGenerator.class;	
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
	public static final int OUTPUT = 8;
	public static final int AGENT = 9;

	public static final String WORKDONE_OP = "_workdone";
	public static final String CODE_FIELD = "_code";
	public static final String MODE_FIELD = "_mode";
	public static final String TYPE_FIELD = "_type";
	public static final String LEFT_SRC = "_srcLeft";	
	public static final String RIGHT_SRC = "_srcRight";
	public static final String SRC = "_src";	
	public static final String OUTPUT_FIELD = "_output";

	@SuppressWarnings("unchecked")
	Class workGenerator = null;
	private String id;
	List<String> inputs = null, lInputs = null, rInputs = null;
	CodeFragment code;
	MPStore datastore;
	int type;
	protected RemusPipeline pipeline = null;
	LinkedList<RemusInstance> activeInstances;
	private AttachStore attachstore;


	public void addInput( String in ) {
		if ( inputs == null )
			inputs = new ArrayList<String>();
		inputs.add(in);
	}	

	public void addLeftInput( String in ) {
		if ( lInputs == null )
			lInputs = new LinkedList<String>();
		lInputs.add(in);
		addInput(in);
	}

	public void addRightInput( String in ) {
		if ( rInputs == null )
			rInputs = new LinkedList<String>();
		rInputs.add(in);
		addInput(in);
	}

	public String getInput() {
		return inputs.get(0);
	}

	public String getLeftInput() {
		return lInputs.get(0);
	}

	public String getRightInput() {
		return rInputs.get(0);
	}

	public CodeFragment getCode() {
		return code;
	}

	public List<String> getInputs() {
		if ( inputs != null )
			return inputs;
		return new ArrayList<String>();
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
			return true;
		if ( hasInputs() ) {
			boolean allReady = true;
			for ( String iRef : inputs ) {
				if ( iRef.compareTo("?") != 0 ) {
					RemusApplet iApplet = getPipeline().getApplet( iRef );
					if ( iApplet != null ) {
						if ( !iApplet.isComplete(remusInstance) ) {
							allReady = false;
						}
					} else {
						allReady = false;
					}
				} else {				
					allReady = true;
				}
			}
			return allReady;
		}		
		return true;
	}

	public long inputTimeStamp( RemusInstance remusInstance ) {
		long out = 0;
		for ( String iRef : inputs ) {
			if ( iRef.compareTo("?") != 0 ) {
				RemusApplet iApplet = getPipeline().getApplet( iRef );
				if ( iApplet != null ) {
					AppletInstanceStatusView status = new AppletInstanceStatusView( iApplet );
					long val = status.getTimeStamp( remusInstance );
					if ( out < val ) {
						out = val;
					}
				}
			}			
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	public boolean isComplete( RemusInstance remusInstance ) {
		boolean found = false;
		for ( Object statObj : datastore.get( getPath() + AppletInstanceStatusView.InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) ) {
			if ( statObj != null && ((Map)statObj).containsKey( WORKDONE_OP ) && (Boolean)((Map)statObj).get(WORKDONE_OP) == true ) {
				found = true;
			}
		}
		if ( found ) {
			for ( @SuppressWarnings("unused") String key : datastore.listKeys(  getPath() + "/@error", remusInstance.toString() ) ) {
				found = false;
			}
		}
		return found;
	}

	public boolean isInError(  RemusInstance remusInstance ) {
		boolean found = false;
		for ( @SuppressWarnings("unused") String key : datastore.listKeys(  getPath() + "/@error", remusInstance.toString() ) ) {
			found = true;
		}
		return found;
	}

	@SuppressWarnings("unchecked")
	public void setComplete(RemusInstance remusInstance) {
		Object statObj = null;
		for ( Object curObj : datastore.get( getPath() + AppletInstanceStatusView.InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) ) {
			statObj = curObj;
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		System.err.println("SET COMPLETE: " + getPath() );
		((Map)statObj).put(WORKDONE_OP, true);
		datastore.add( getPath() + AppletInstanceStatusView.InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR, 0, 0, remusInstance.toString(), statObj );
		//datastore.delete( getPath() + "/@done", remusInstance.toString() );
	}

	@SuppressWarnings("unchecked")
	public void unsetComplete(RemusInstance remusInstance) {
		Object statObj = null;
		for ( Object curObj : datastore.get( getPath() + AppletInstanceStatusView.InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) ) {
			statObj = curObj;
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		System.err.println("UNSET COMPLETE: " + getPath() );
		((Map)statObj).put(WORKDONE_OP, false);
		datastore.add( getPath() + AppletInstanceStatusView.InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR, 0, 0, remusInstance.toString(), statObj );
		//datastore.delete( getPath() + "/@done", remusInstance.toString() );
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
		AppletInstanceStatusView thisStat = new AppletInstanceStatusView(this);
		HashMap<AppletInstance,Set<WorkKey>> out = new HashMap<AppletInstance,Set<WorkKey>>();		
		//for ( RemusInstance inst : getActiveInstanceList() ) {
		for ( RemusInstance inst : getInstanceList() ) {
			//System.err.println( "APPLET SCAN:" + getPath() + " " + inst.toString() );
			if ( out.size() < maxListSize ) {
				if ( !isComplete(inst) ) {
					if ( isReady(inst)) {
						if ( workGenerator != null ) {
							try {
								WorkGenerator gen = (WorkGenerator) workGenerator.newInstance();
								Set<WorkKey> workSet =  gen.getActiveKeys(this, inst, maxListSize - out.size());
								System.err.println("GENERATE WORK: " + getPath() + " " + inst.toString() + " COUNT:" + workSet.size());
								if ( gen.isDone() ) {
									setComplete(inst);
								} else {
									AppletInstance ai =  gen.getAppletInstance();
									assert ai != null;
									out.put( ai, workSet );								
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
				} else {
					if ( hasInputs() ) {
						long thisTime = thisStat.getTimeStamp(inst);
						long inTime = inputTimeStamp(inst);
						//System.err.println( this.getPath() + ":" + thisTime + "  " + "IN:" + inTime );			
						if ( inTime > thisTime ) {
							System.err.println( "YOUNG INPUT:" + getPath() );
							unsetComplete(inst);
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


	public Collection<RemusInstance> getInstanceList() {
		Collection<RemusInstance> out = new HashSet<RemusInstance>( );
		for ( String key : datastore.listKeys( getPath() + AppletInstanceStatusView.InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR ) ) {
			out.add( new RemusInstance( key ) );
		}
		return out;
	}


	@SuppressWarnings("unchecked")
	public boolean createInstance(String submitKey, Map params, RemusInstance inst) {

		if ( datastore.containsKey( getPath() + AppletInstanceStatusView.InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR, inst.toString()) ) {
			return false;
		}

		Map baseMap = new HashMap();

		if ( params != null ) {
			for ( Object key : params.keySet() ) {
				baseMap.put(key, params.get(key) );
			}
		}

		for ( Object i : datastore.get( "/" + pipeline.getID() + "/@pipeline" , RemusInstance.STATIC_INSTANCE_STR, getID() ) ) {
			for ( Object key : ((Map)i).keySet() ) {
				baseMap.put(key, ((Map)i).get(key) );
			}
		}
		if ( baseMap == null )	
			baseMap = new HashMap();
		baseMap.put("_instance", inst.toString());
		baseMap.put("_submitKey", submitKey);

		if ( getType() == MERGER || getType() == MATCHER ) {
			Map inMap = new HashMap();
			Map lMap = new HashMap();
			Map rMap = new HashMap();
			lMap.put("_instance", inst.toString());
			lMap.put("_applet",getLeftInput() );
			rMap.put("_instance", inst.toString());
			rMap.put("_applet",getRightInput() );
			inMap.put("_left", lMap);
			inMap.put("_right", rMap);				
			inMap.put("_axis", "_left");
			baseMap.put("_input", inMap);
		} else if ( getType() == AGENT ) {			
			Map inMap = new HashMap();
			inMap.put("_instance", "@agent");
			inMap.put("_applet", getInput() );
			baseMap.put("_input", inMap);
		} else if ( getType() == PIPE ) {
			List outList = new ArrayList();
			for ( String input : getInputs() ) {
				Map inMap = new HashMap();
				inMap.put("_instance", inst.toString());
				inMap.put("_applet", input );
				outList.add( inMap );
			}
			baseMap.put("_input", outList);			
		} else if ( hasInputs() && getInput().compareTo("?") != 0 ) {
			Map inMap = new HashMap();
			inMap.put("_instance", inst.toString());
			inMap.put("_applet", getInput() );
			baseMap.put("_input", inMap);			
		}

		//remove fields that will be added in automatically later
		baseMap.remove("_timestamp");
		baseMap.remove("_totalCount");
		baseMap.remove("_errorCount");
		baseMap.remove("_workdone");

		if ( getType() == STORE || getType() == AGENT ) {
			baseMap.put(WORKDONE_OP, true);
		}

		AppletInstanceStatusView stat = new AppletInstanceStatusView(this);
		stat.updateStatus(inst, baseMap);
		return true;
	};

	public void deleteInstance(RemusInstance instance) {
		datastore.delete(getPath(), instance.toString() );		
		datastore.delete(getPath() + AppletInstanceStatusView.InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR, instance.toString() );		
		datastore.delete(getPath() + "/@done", instance.toString() );		
		datastore.delete(getPath() + "/@error", instance.toString() );
		attachstore.delete(getPath(), instance.toString() );
	}

	public void errorWork(RemusInstance inst, long jobID, String workerID, String error) {
		datastore.add( getPath() + "/@error", inst.toString(), 0L, 0L, Long.toString(jobID), error);
	}

	public void deleteErrors(RemusInstance inst) {
		datastore.delete(getPath() + "/@error", inst.toString() );		
	};



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


}
