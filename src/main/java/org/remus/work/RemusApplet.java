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
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.manage.WorkStatus;
import org.remus.serverNodes.AppletInstanceStatusView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * 
 * @author kellrott
 *
 * Base class for Applets
 */
public class RemusApplet {

	public static RemusApplet newApplet( String id, String type, int mode ) {
		RemusApplet out = new RemusApplet();

		switch (mode) {
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
			out.id = id;
			out.mode = mode;
			out.type = type;
			out.inputs = null;
			out.activeInstances = new LinkedList<RemusInstance>();			
		}
		out.logger = LoggerFactory.getLogger(RemusApplet.class);
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

	public static final String CODE_FIELD = "_code";
	public static final String MODE_FIELD = "_mode";
	public static final String TYPE_FIELD = "_type";
	public static final String LEFT_SRC = "_srcLeft";	
	public static final String RIGHT_SRC = "_srcRight";
	public static final String SRC = "_src";	
	public static final String OUTPUT_FIELD = "_output";

	Logger logger;

	@SuppressWarnings("unchecked")
	Class workGenerator = null;
	private String id;
	List<String> inputs = null, lInputs = null, rInputs = null;
	MPStore datastore;
	int mode;
	private String type;
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

	
	public String getType() {
		return type;
	}

	
	public List<String> getInputs() {
		if ( inputs != null )
			return inputs;
		return new ArrayList<String>();
	}


	public String getPath() {
		return "/" + pipeline.getID() + "/" + id;
	}

	public void setPipeline(RemusPipeline remusPipeline) {
		this.pipeline = remusPipeline;		
		this.datastore = remusPipeline.getDataStore();
		this.attachstore = remusPipeline.getAttachStore();
	}

	public RemusPipeline getPipeline() {
		return this.pipeline;
	}


	public boolean isReady( RemusInstance remusInstance ) {
		if ( mode==STORE )
			return true;
		if ( hasInputs() ) {
			boolean allReady = true;
			for ( String iRef : inputs ) {
				if ( iRef.compareTo("?") != 0 ) {
					RemusApplet iApplet = getPipeline().getApplet( iRef );
					if ( iApplet != null ) {
						if ( iApplet.getMode() != STORE ) {
							if ( ! WorkStatus.isComplete(iApplet, remusInstance) ) {
								allReady = false;
							}
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
					long val = WorkStatus.getTimeStamp( this, remusInstance );
					if ( out < val ) {
						out = val;
					}
				}
			}			
		}
		return out;
	}

	public long getDataTimeStamp( RemusInstance remusInstance ) {
		return datastore.getTimeStamp(getPath(), remusInstance.toString());
	}


	public boolean isInError(  RemusInstance remusInstance ) {
		boolean found = false;
		for ( @SuppressWarnings("unused") String key : datastore.listKeys(  getPath() + "/@error", remusInstance.toString() ) ) {
			found = true;
		}
		return found;
	}


	public boolean hasInputs() {
		if ( inputs == null )
			return false;
		return true;
	}


	public int getMode() {
		return mode;
	}

	public Set<WorkStatus> getWorkList() {
		HashSet<WorkStatus> out = new HashSet<WorkStatus>();		
		for ( RemusInstance inst : getInstanceList() ) {
			if ( !WorkStatus.isComplete(this, inst) ) {
				if ( isReady(inst)) {
					if ( workGenerator != null ) {
						long infoTime = WorkStatus.getTimeStamp(this, inst);
						long dataTime = getDataTimeStamp(inst);
						if ( infoTime < dataTime || !WorkStatus.hasStatus( this, inst ) ) {
							try {
								logger.info("GENERATE WORK: " + getPath() + " " + inst.toString() );
								WorkGenerator gen = (WorkGenerator) workGenerator.newInstance();								
								gen.writeWorkTable(this, inst);
							} catch (InstantiationException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							} catch (IllegalAccessException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}	
						} else {
							logger.info( "Active Work Stack: " + inst.toString() + ":" + this.getID() );
						}
						out.add( new WorkStatus(inst, this) );
					}

				}
			} else {
				if ( hasInputs() ) {
					long thisTime = WorkStatus.getTimeStamp(this, inst);
					long inTime = inputTimeStamp(inst);
					//System.err.println( this.getPath() + ":" + thisTime + "  " + "IN:" + inTime );			
					if ( inTime > thisTime ) {
						logger.info( "YOUNG INPUT (applet reset):" + getPath() );
						WorkStatus.unsetComplete(this, inst);
					}
				}
			}
		}
		return out;
	}

	public void finishWork(RemusInstance remusInstance, long jobID, String workerName, long emitCount) {
		datastore.add(getPath() + "/@done", remusInstance.toString(), 0L, 0L, Long.toString(jobID), workerName);
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
		if (baseMap == null) {
			baseMap = new HashMap();
		}
		baseMap.put("_instance", inst.toString());
		baseMap.put("_submitKey", submitKey);

		if (getMode() == MERGER || getMode() == MATCHER) {
			Map inMap = new HashMap();
			Map lMap = new HashMap();
			Map rMap = new HashMap();
			lMap.put("_instance", inst.toString());
			lMap.put("_applet", getLeftInput());
			rMap.put("_instance", inst.toString());
			rMap.put("_applet", getRightInput());
			inMap.put("_left", lMap);
			inMap.put("_right", rMap);				
			inMap.put("_axis", "_left");
			baseMap.put("_input", inMap);
		} else if (getMode() == AGENT) {
			Map inMap = new HashMap();
			inMap.put("_instance", "@agent");
			inMap.put("_applet", getInput() );
			baseMap.put("_input", inMap);
		} else if (getMode() == PIPE) {
			if ( getInput().compareTo("?") != 0 ) {
				List outList = new ArrayList();
				for ( String input : getInputs() ) {
					Map inMap = new HashMap();
					inMap.put("_instance", inst.toString());
					inMap.put("_applet", input );
					outList.add( inMap );
				}
				baseMap.put("_input", outList);
			}
		} else if ( hasInputs() && getInput().compareTo("?") != 0 ) {
			Map inMap = new HashMap();
			inMap.put("_instance", inst.toString());
			inMap.put("_applet", getInput() );
			baseMap.put("_input", inMap);			
		}

		if (getMode() == STORE || getMode() == AGENT) {
			//	baseMap.put(WORKDONE_OP, true);
		}

		AppletInstanceStatusView.updateStatus(this, inst, baseMap);
		return true;
	};

	public void deleteInstance(RemusInstance instance) {
		datastore.delete(getPath(), instance.toString() );		
		datastore.delete(getPath() + AppletInstanceStatusView.InstanceStatusName,
				RemusInstance.STATIC_INSTANCE_STR, instance.toString());
		datastore.delete(getPath() + WorkStatus.WorkStatusName, 
				RemusInstance.STATIC_INSTANCE_STR, instance.toString());
		datastore.delete(getPath() + "/@done", instance.toString());
		datastore.delete(getPath() + "/@work", instance.toString());
		datastore.delete(getPath() + "/@error", instance.toString());
		attachstore.delete(getPath(), instance.toString());
	}

	public void errorWork(RemusInstance inst, long jobID, String workerID, String error) {
		datastore.add(getPath() + "/@error", inst.toString(), 0L, 0L, Long.toString(jobID), error);
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
