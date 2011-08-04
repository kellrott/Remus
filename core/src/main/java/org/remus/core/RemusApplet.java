package org.remus.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.serverNodes.AppletInstanceStatusView;
import org.remus.thrift.AppletRef;
import org.remus.work.AgentGenerator;
import org.remus.work.MapGenerator;
import org.remus.work.MatchGenerator;
import org.remus.work.MergeGenerator;
import org.remus.work.PipeGenerator;
import org.remus.work.ReduceGenerator;
import org.remus.work.SplitGenerator;
import org.remus.work.WorkGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemusApplet {

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
	RemusDB datastore;
	int mode;
	private String type;
	LinkedList<RemusInstance> activeInstances;
	private RemusAttach attachstore;
	private RemusPipeline pipeline;


	public RemusApplet(RemusPipeline pipeline, String name, RemusDB datastore) {
		logger = LoggerFactory.getLogger(RemusApplet.class);
		id = name;
		pipeline = pipeline;

		/*
		if ( out != null ) {
			out.id = id;
			out.mode = mode;
			out.type = type;
			out.inputs = null;
			out.activeInstances = new LinkedList<RemusInstance>();			
		}
		 */

	}


	void setMode(int mode) {
		switch (mode) {
		case MAPPER: {
			workGenerator = MapGenerator.class; //new MapperApplet();	
			break;
		}
		case REDUCER: {
			workGenerator = ReduceGenerator.class;	
			break;
		}
		case SPLITTER: {
			workGenerator = SplitGenerator.class;	
			break;
		}
		case MERGER: {
			workGenerator = MergeGenerator.class;	
			break;
		}
		case MATCHER: {
			workGenerator = MatchGenerator.class;	
			break;
		}
		case PIPE: {
			workGenerator = PipeGenerator.class;	
			break;
		}
		case AGENT: {
			workGenerator = AgentGenerator.class;	
			break;			
		}	
		}
	}


	public void load(Map appletObj) {

		String mode = (String)appletObj.get( MODE_FIELD );
		String codeType = (String)appletObj.get( TYPE_FIELD);

		Integer appletType = null;
		if (mode.compareTo("map") == 0) {
			appletType = MAPPER;
		}
		if (mode.compareTo("reduce") == 0) {
			appletType = REDUCER;
		}
		if ( mode.compareTo("pipe") == 0 ) {
			appletType = PIPE;
		}
		if ( mode.compareTo("merge") == 0 ) {
			appletType = MERGER;
		}
		if ( mode.compareTo("match") == 0 ) {
			appletType = MATCHER;
		}
		if ( mode.compareTo("split") == 0 ) {
			appletType = SPLITTER;
		}
		if ( mode.compareTo("store") == 0 ) {
			appletType = STORE;
		}
		if ( mode.compareTo("agent") == 0 ) {
			appletType = AGENT;
		}
		if (appletType == null)
			return; //TODO: raise exception

		if ( appletType == MATCHER || appletType == MERGER ) {
			//try {
			String lInput = (String) appletObj.get( LEFT_SRC );
			//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
			addLeftInput( lInput );
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
			//try {
			String rInput = (String) appletObj.get( RIGHT_SRC );
			//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
			addRightInput(rInput);
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
		} else {
			//try {
			Object src = appletObj.get( SRC );

			if ( src instanceof String ) {
				String input = (String) src;
				//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
				addInput(input);
			}
			if ( src instanceof List ) {
				for ( Object obj : (List)  src )	{
					//RemusPath path = new RemusPath( this, (String)obj, pipelineName, name );
					addInput((String)obj);
				}
			}
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
		}
		
	}

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


	public int getMode() {
		return mode;
	}

	public boolean hasInputs() {
		if ( inputs == null )
			return false;
		return true;
	}



	public Set<WorkStatus> getWorkList() {
		HashSet<WorkStatus> out = new HashSet<WorkStatus>();		
		for ( RemusInstance inst : getInstanceList() ) {
			if ( !WorkStatus.isComplete(pipeline, this, inst) ) {
				if ( isReady(inst)) {
					if ( workGenerator != null ) {
						long infoTime = WorkStatus.getTimeStamp(pipeline, this, inst);
						try {
							long dataTime = getDataTimeStamp(inst);
							if ( infoTime < dataTime || !WorkStatus.hasStatus( pipeline, this, inst ) ) {
								try {
									logger.info("GENERATE WORK: " + pipeline.getID() +"/" + getID() + " " + inst.toString() );
									WorkGenerator gen = (WorkGenerator) workGenerator.newInstance();								
									gen.writeWorkTable(pipeline, this, inst, datastore);
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
							out.add( new WorkStatus(pipeline, inst, this) );
						} catch ( TException e ) {
							e.printStackTrace();
						}
					}

				}
			} else {
				if ( hasInputs() ) {
					long thisTime = WorkStatus.getTimeStamp(pipeline, this, inst);
					long inTime = inputTimeStamp(inst);
					//System.err.println( this.getPath() + ":" + thisTime + "  " + "IN:" + inTime );			
					if ( inTime > thisTime ) {
						logger.info( "YOUNG INPUT (applet reset):" + getID() );
						WorkStatus.unsetComplete(pipeline, this, inst);
					}
				}
			}
		}
		return out;
	}


	public Collection<RemusInstance> getInstanceList() {
		Collection<RemusInstance> out = new HashSet<RemusInstance>( );
		AppletRef applet = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, getID() + AppletInstanceStatusView.InstanceStatusName );
		for ( String key : datastore.listKeys( applet ) ){
			out.add( new RemusInstance( key ) );
		}
		return out;
	}


	public void deleteInstance(RemusInstance instance) throws TException {
		AppletRef applet = new AppletRef(pipeline.getID(), instance.toString(), getID() );
		datastore.deleteStack( applet );
		attachstore.deleteStack( applet );
		applet.applet = getID() + "/@done";
		datastore.deleteStack( applet );
		applet.applet = getID() + "/@work";
		datastore.deleteStack( applet );
		applet.applet = getID() + "/@error";
		datastore.deleteStack( applet );

		applet.instance = RemusInstance.STATIC_INSTANCE_STR;
		applet.applet = getID() + AppletInstanceStatusView.InstanceStatusName;
		datastore.deleteStack( applet );
		applet.applet = getID() + WorkStatus.WorkStatusName;
		datastore.deleteStack( applet );
	}




	@SuppressWarnings("unchecked")
	public boolean createInstance(String submitKey, Map params, RemusInstance inst) throws TException {
		AppletRef instApplet = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, getID() + AppletInstanceStatusView.InstanceStatusName );

		if ( datastore.containsKey( instApplet, inst.toString()) ) {
			return false;
		}

		Map baseMap = new HashMap();

		if ( params != null ) {
			for ( Object key : params.keySet() ) {
				baseMap.put(key, params.get(key) );
			}
		}

		AppletRef pipelineApplet = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, getID() + "/@pipeline" );

		for ( Object i : datastore.get( pipelineApplet, getID() ) ) {
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

	

	public void errorWork(RemusInstance inst, long jobID, String workerID, String error) throws TException {
		AppletRef applet = new AppletRef(pipeline.getID(), inst.toString(), getID() + "/@error");
		datastore.add(applet, 0L, 0L, Long.toString(jobID), error);
	}

	public void deleteErrors(RemusInstance inst) throws TException {
		AppletRef applet = new AppletRef(pipeline.getID(), inst.toString(), getID() + "/@error");		
		datastore.deleteStack( applet );		
	};



	@Override
	public int hashCode() { 
		return getID().hashCode();
	};

	@Override
	public boolean equals(Object obj) {
		RemusApplet a = (RemusApplet)obj;
		return a.getID().equals(getID());
	}

	public RemusDB getDataStore() {
		return datastore;
	}

	public String getID() {
		return id;
	}

	public RemusAttach getAttachStore() {
		return getAttachStore();
	}


}
