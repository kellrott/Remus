package org.remus.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.remus.thrift.Constants;
import org.apache.thrift.TException;
import org.json.simple.JSONAware;
import org.remus.JSON;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.WorkMode;
import org.remus.work.AgentGenerator;
import org.remus.work.MapGenerator;
import org.remus.work.MatchGenerator;
import org.remus.work.MergeGenerator;
import org.remus.work.PipeGenerator;
import org.remus.work.ReMapGenerator;
import org.remus.work.ReReduceGenerator;
import org.remus.work.ReduceGenerator;
import org.remus.work.SplitGenerator;
import org.remus.work.WorkGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RemusApplet implements JSONAware, Comparable<RemusApplet> {

	public static final int MAPPER = WorkMode.MAP.getValue();
	public static final int MERGER = WorkMode.MERGE.getValue();
	public static final int MATCHER = WorkMode.MATCH.getValue();
	public static final int SPLITTER = WorkMode.SPLIT.getValue();
	public static final int REDUCER = WorkMode.REDUCE.getValue();
	public static final int PIPE = WorkMode.PIPE.getValue();
	public static final int STORE = WorkMode.STORE.getValue();
	public static final int OUTPUT = WorkMode.OUTPUT.getValue();
	public static final int AGENT = WorkMode.AGENT.getValue();
	public static final int REMAPPER = WorkMode.REMAP.getValue();
	public static final int REREDUCER = WorkMode.REREDUCE.getValue();

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
	List<String> sources = null, lSources = null, rSources = null;
	int mode;
	private String type;
	private RemusPipeline pipeline;

	private RemusDB datastore;
	private RemusAttach attachstore;
	private ArrayList<String> outputs;
	private Map appletDesc;

	public RemusApplet(RemusPipeline pipeline, String name, RemusDB datastore, RemusAttach attachstore) throws TException, NotImplemented, RemusDatabaseException {
		logger = LoggerFactory.getLogger(RemusApplet.class);
		id = name;
		this.pipeline = pipeline;
		this.datastore = datastore;
		this.attachstore = attachstore;

		AppletRef arApplet = new AppletRef(pipeline.getID(), 
				RemusInstance.STATIC_INSTANCE_STR, Constants.PIPELINE_APPLET);

		for (Object obj : datastore.get(arApplet, name)) {
			appletDesc = (Map)obj;
		}
		if (appletDesc == null) {
			throw new RemusDatabaseException("Applet Description not found");
		}
		load((Map) appletDesc);

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
		if (mode == MAPPER) {
			workGenerator = MapGenerator.class;
		} else if (mode == REDUCER) {	
			workGenerator = ReduceGenerator.class;	
		} else if (mode == SPLITTER) {	
			workGenerator = SplitGenerator.class;	
		} else if (mode == MERGER) {	
			workGenerator = MergeGenerator.class;	
		} else if (mode == MATCHER) {	
			workGenerator = MatchGenerator.class;	
		} else if (mode == PIPE) {	
			workGenerator = PipeGenerator.class;	
		} else if (mode == AGENT) {	
			workGenerator = AgentGenerator.class;	
		} else if (mode == REMAPPER) {
			workGenerator = ReMapGenerator.class;
		} else if (mode == REREDUCER) {
			workGenerator = ReReduceGenerator.class;
		} else {
			workGenerator = null;	
		}
		this.mode = mode;
	}


	public void load(Map appletObj) throws RemusDatabaseException {

		String modeStr = (String) appletObj.get(MODE_FIELD);
		if (modeStr == null) {
			throw new RemusDatabaseException("Missing _mode field");
		}
		type = (String) appletObj.get(TYPE_FIELD);

		Integer appletType = null;
		if (modeStr.compareTo("map") == 0) {
			appletType = MAPPER;
		}
		if (modeStr.compareTo("reduce") == 0) {
			appletType = REDUCER;
		}
		if (modeStr.compareTo("pipe") == 0) {
			appletType = PIPE;
		}
		if (modeStr.compareTo("merge") == 0) {
			appletType = MERGER;
		}
		if (modeStr.compareTo("match") == 0) {
			appletType = MATCHER;
		}
		if (modeStr.compareTo("split") == 0) {
			appletType = SPLITTER;
		}
		if (modeStr.compareTo("store") == 0) {
			appletType = STORE;
		}
		if (modeStr.compareTo("agent") == 0) {
			appletType = AGENT;
		}
		if (modeStr.compareTo("output") == 0) {
			appletType = OUTPUT;
		}
		if (modeStr.compareTo("remap") == 0) {
			appletType = REMAPPER;
		}
		if (modeStr.compareTo("rereducer") == 0) {
			appletType = REREDUCER;
		}

		if (appletType == null) {
			throw new RemusDatabaseException("Invalid Applet Type");
		}

		setMode(appletType);
		if (appletType == MATCHER || appletType == MERGER) {
			//try {
			String lInput = (String) appletObj.get(LEFT_SRC);
			//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
			addLeftSource(lInput);
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
			//try {
			String rInput = (String) appletObj.get(RIGHT_SRC);
			//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
			addRightSource(rInput);
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
		} else {
			//try {
			Object src = appletObj.get(SRC);

			if (src instanceof String) {
				String input = (String) src;
				//RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
				addSource(input);
			}
			if (src instanceof List) {
				for (Object obj : (List) src) {
					//RemusPath path = new RemusPath( this, (String)obj, pipelineName, name );
					addSource((String) obj);
				}
			}
			//} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
		}

		if (appletObj.containsKey(OUTPUT_FIELD)) {
			List outs = (List) appletObj.get(OUTPUT_FIELD);
			for (Object outName : outs) {
				addOutput((String) outName);
			}
		}		
	}



	private void addOutput(String outName) {
		if (outputs == null) {
			outputs = new ArrayList<String>();
		}
		outputs.add(outName);
	}

	private void addSource(String in) {
		if (sources == null) {
			sources = new ArrayList<String>();
		}
		sources.add(in);
	}	

	private void addLeftSource(String in) {
		if (lSources == null) {
			lSources = new LinkedList<String>();
		}
		lSources.add(in);
		addSource(in);
	}

	private void addRightSource(String in) {
		if (rSources == null) {
			rSources = new LinkedList<String>();
		}
		rSources.add(in);
		addSource(in);
	}

	public String getSource() {
		return sources.get(0);
	}

	public String getLeftSource() {
		return lSources.get(0);
	}

	public String getRightSource() {
		return rSources.get(0);
	}


	public String getType() {
		return type;
	}


	public List<String> getSources() {
		if ( sources != null )
			return sources;
		return new ArrayList<String>();
	}


	public int getMode() {
		return mode;
	}

	public boolean hasSources() {
		if (sources == null) {
			return false;
		}
		return true;
	}



	public Set<AppletInstance> getActiveApplets(RemusInstance inst) {
		HashSet<AppletInstance> out = new HashSet<AppletInstance>();		
		AppletInstance ai = new AppletInstance(pipeline, inst, this, datastore);
		if (!ai.isComplete()) {
			if (ai.isReady()) {
				if (workGenerator != null) {
					try {
						long infoTime = ai.getStatusTimeStamp();
						long dataTime = ai.inputTimeStamp();
						if (infoTime < dataTime || !WorkStatus.hasStatus(pipeline, this, inst)) {
							try {
								logger.info("GENERATE WORK: " + pipeline.getID() + "/" + getID() + " " + inst.toString());
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
							//logger.info("Active Work Stack: " + inst.toString() + ":" + this.getID());
						}
						out.add(ai);
					} catch (TException e) {
						e.printStackTrace();
					} catch (NotImplemented e) {
						e.printStackTrace();
					}
				}

			}
		} else {
			if (hasSources()) {
				try {
					long thisTime = ai.getStatusTimeStamp();
					long inTime = ai.inputTimeStamp();
					//System.err.println( this.getPath() + ":" + thisTime + "  " + "IN:" + inTime );			
					if (inTime > thisTime) {
						logger.info("YOUNG INPUT (applet reset):" + inst + ":" + getID() );
						WorkStatus.unsetComplete(pipeline, this, inst);
					}
				} catch (TException e){
					e.printStackTrace();
				} catch (NotImplemented e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return out;
	}


	public Collection<RemusInstance> getInstanceList() {
		Collection<RemusInstance> out = new HashSet<RemusInstance>();
		AppletRef applet = new AppletRef(pipeline.getID(), 
				RemusInstance.STATIC_INSTANCE_STR, 
				Constants.INSTANCE_APPLET);
		for (String key : datastore.listKeys(applet)) {
			//BUG possible name match clashes
			if (key.endsWith(":" + getID())) {
				out.add(new RemusInstance(key.split(":")[0]));
			}
		}
		return out;
	}


	public void deleteInstance(RemusInstance instance) throws TException, NotImplemented {
		AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), getID());

		logger.debug("DELETE:" + ar);
		datastore.deleteStack(ar);
		ar.applet = getID() + Constants.DONE_APPLET;
		datastore.deleteStack(ar);
		ar.applet = getID() + Constants.WORK_APPLET;
		datastore.deleteStack(ar);
		ar.applet = getID() + Constants.ERROR_APPLET;
		datastore.deleteStack(ar);

		ar.instance = RemusInstance.STATIC_INSTANCE_STR;
		ar.applet = Constants.INSTANCE_APPLET;
		datastore.deleteValue(ar, instance.toString() + ":" + getID());
		ar.applet = getID() + Constants.WORK_APPLET;
		datastore.deleteValue(ar, instance.toString());

		if (attachstore != null) {
			ar.applet = getID();
			ar.instance = instance.toString();
			attachstore.deleteStack(ar);
		}
	}

	private static final List<String> suppressFields = Arrays.asList("_instance", "_code", "_script", "_src", "_pipeline");


	@SuppressWarnings("unchecked")
	//public boolean createInstance(String submitKey, PipelineSubmission params, RemusInstance inst) throws TException, NotImplemented {
	public boolean createInstance(PipelineSubmission params, RemusInstance inst) throws TException, NotImplemented {

		logger.info("Creating instance of " + getID() + " for " + inst.toString());
		AppletRef instApplet = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, Constants.INSTANCE_APPLET);

		if (datastore.containsKey(instApplet, inst.toString() + ":" + getID())) {
			return false;
		}

		Map baseMap = new HashMap();

		if (params != null) {
			for (Object key : params.base.keySet()) {
				if ( !suppressFields.contains(key)) {
					baseMap.put(key, params.base.get(key));
				}
			}
		}
		
		baseMap.put("_instance", inst.toString());
		baseMap.put("_applet", getID());
		baseMap.put("_pipeline",pipeline.getID());
		
		
		for (Object key : appletDesc.keySet()) {
			baseMap.put(key, appletDesc.get(key));
		}

		if (getMode() == MERGER || getMode() == MATCHER) {
			Map inMap = new HashMap();
			Map lMap = new HashMap();
			Map rMap = new HashMap();
			lMap.put("_instance", inst.toString());
			lMap.put("_applet", getLeftSource());
			rMap.put("_instance", inst.toString());
			rMap.put("_applet", getRightSource());
			inMap.put("_left", lMap);
			inMap.put("_right", rMap);				
			inMap.put("_axis", "_left");
			baseMap.put("_input", inMap);
		} else if (getMode() == AGENT) {
			Map inMap = new HashMap();
			inMap.put("_instance", RemusInstance.STATIC_INSTANCE_STR);
			inMap.put("_applet", "/@agent?" + pipeline.getID());
			baseMap.put("_input", inMap);
		} else if (getMode() == PIPE || getMode() == REMAPPER || getMode() == REREDUCER) {
			if (getSource().compareTo("?") != 0) {
				List outList = new ArrayList();
				for (String input : getSources()) {
					Map inMap = new HashMap();
					inMap.put("_instance", inst.toString());
					inMap.put("_applet", input);
					outList.add(inMap);
				}
				baseMap.put("_input", outList);
			}
		} else if (hasSources() && getSource().compareTo("?") != 0) {
			Map inMap = new HashMap();
			inMap.put("_instance", inst.toString());
			inMap.put("_applet", getSource());
			baseMap.put("_input", inMap);			
		}

		if (getMode() == STORE || getMode() == AGENT) {
			//	baseMap.put(WORKDONE_OP, true);
		}

		PipelineSubmission instInfo = new PipelineSubmission(baseMap);

		if (outputs != null) {
			for (String output : outputs) {
				try {
					PipelineSubmission outputInfo = new PipelineSubmission(new HashMap(baseMap));
					outputInfo.setInstance(inst);
					outputInfo.setMode("output");
					RemusApplet outApplet = new RemusApplet(pipeline, getID() + ":" + output, datastore, attachstore);
					AppletInstance ai = new AppletInstance(pipeline, inst, outApplet, datastore);
					ai.updateInstanceInfo(outputInfo);
				} catch (RemusDatabaseException e) {
				}
			}
		}
		AppletInstance ai = new AppletInstance(pipeline, inst, this, datastore);
		ai.updateInstanceInfo(instInfo);
		return true;
	};



	public void errorWork(RemusInstance inst, long jobID, String workerID, String error) throws TException, NotImplemented {
		AppletRef applet = new AppletRef(pipeline.getID(), inst.toString(), getID() + Constants.ERROR_APPLET);
		datastore.add(applet, 0L, 0L, Long.toString(jobID), error);
	}

	public void deleteErrors(RemusInstance inst) throws TException, NotImplemented {
		AppletRef applet = new AppletRef(pipeline.getID(), inst.toString(), getID() + Constants.ERROR_APPLET);
		datastore.deleteStack(applet);
	};



	@Override
	public int hashCode() { 
		return getID().hashCode();
	};

	
	@Override
	public boolean equals(Object obj) {
		RemusApplet a = (RemusApplet) obj;
		return a.getID().equals(getID());
	}

	@Override
	public int compareTo(RemusApplet o) {
		return getID().compareTo(o.getID());
	}
	
	public RemusDB getDataStore() {
		return datastore;
	}

	public String getID() {
		return id;
	}

	public RemusAttach getAttachStore() {
		return attachstore;
	}

	public AppletInstance getAppletInstance(String inst) throws TException, NotImplemented {
		AppletInstance ai = new AppletInstance(pipeline, RemusInstance.getInstance(datastore, pipeline.getID(), inst), this, datastore);
		return ai;
	}

	@Override
	public String toJSONString() {
		return JSON.dumps(appletDesc);
	}

}
