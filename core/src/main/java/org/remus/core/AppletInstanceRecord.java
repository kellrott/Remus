package org.remus.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.json.simple.JSONAware;
import org.remus.JSON;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
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


public class AppletInstanceRecord implements JSONAware {

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
	public static final String AUTO_FIELD = "_auto";
	public static final String LEFT_SRC = "_srcLeft";
	public static final String RIGHT_SRC = "_srcRight";
	public static final String SRC = "_src";
	public static final String OUTPUT_FIELD = "_output";
	public static final String INPUT_FIELD = "_input";
	public static final Object SOURCE_FIELD = "_src";
	public static final Object PIPELINE_FIELD = "_pipeline";
	public static final Object INSTANCE_FIELD = "_instance";
	public static final Object APPLET_FIELD = "_applet";


	private Map base;

	public AppletInstanceRecord(Object obj) {
		this.base = (Map)obj;
	}


	public int getMode() throws RemusDatabaseException {

		String modeStr = (String) base.get(MODE_FIELD);
		if (modeStr == null) {
			throw new RemusDatabaseException("Missing _mode field");
		}

		Integer appletType = null;
		if (modeStr.compareTo("map") == 0) {
			appletType = AppletInstanceRecord.MAPPER;
		}
		if (modeStr.compareTo("reduce") == 0) {
			appletType = AppletInstanceRecord.REDUCER;
		}
		if (modeStr.compareTo("pipe") == 0) {
			appletType = AppletInstanceRecord.PIPE;
		}
		if (modeStr.compareTo("merge") == 0) {
			appletType = AppletInstanceRecord.MERGER;
		}
		if (modeStr.compareTo("match") == 0) {
			appletType = AppletInstanceRecord.MATCHER;
		}
		if (modeStr.compareTo("split") == 0) {
			appletType = AppletInstanceRecord.SPLITTER;
		}
		if (modeStr.compareTo("store") == 0) {
			appletType = AppletInstanceRecord.STORE;
		}
		if (modeStr.compareTo("agent") == 0) {
			appletType = AppletInstanceRecord.AGENT;
		}
		if (modeStr.compareTo("output") == 0) {
			appletType = AppletInstanceRecord.OUTPUT;
		}
		if (modeStr.compareTo("remap") == 0) {
			appletType = AppletInstanceRecord.REMAPPER;
		}
		if (modeStr.compareTo("rereducer") == 0) {
			appletType = AppletInstanceRecord.REREDUCER;
		}

		if (appletType == null) {
			throw new RemusDatabaseException("Invalid Applet Type");
		}
		return appletType;
	}

	/*
	private void addOutput(String outName) {
		if ( !base.containsKey(OUTPUT_FIELD) ) {
			base.put(OUTPUT_FIELD, new ArrayList<String>());
		}
		((List)base.get(OUTPUT_FIELD)).add(outName);
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
	 */

	public String getSource() {
		if (base.get(SOURCE_FIELD) instanceof String) {
			return (String)base.get(SOURCE_FIELD);
		}
		return (String) ((List)base.get(SOURCE_FIELD)).get(0);
	}

	public String getLeftSource() {
		return (String) base.get(LEFT_SRC);
	}

	public String getRightSource() {
		return (String) base.get(RIGHT_SRC);
	}


	public String getType() {
		return (String) base.get(TYPE_FIELD);
	}


	public List<String> getSources() {
		if (base.get(SOURCE_FIELD) instanceof String) {
			return Arrays.asList( (String)base.get(SOURCE_FIELD) );
		}
		return (List) base.get(SOURCE_FIELD);
	}


	public boolean hasSources() {
		return base.containsKey(SOURCE_FIELD);
	}

	public boolean isAuto() {
		if (!base.containsKey(AUTO_FIELD)) {
			return true;
		}
		return (Boolean) base.get(AUTO_FIELD);
	}

	public String getPipeline() {
		return (String)base.get(PIPELINE_FIELD);
	}

	public String getApplet() {
		return (String)base.get(APPLET_FIELD);
	}

	public String getInstance() {
		return (String)base.get(INSTANCE_FIELD);
	}

	public AppletInput getInput(String source, RemusDB datasource) {
		if (!base.containsKey(INPUT_FIELD)) {
			return null;
		}
		Map input = (Map) ((Map) base.get(INPUT_FIELD)).get(source);
		if (input == null) {
			return null;
		}
		try {
			String pipeline = (String)input.get("_pipeline");
			if (pipeline == null) {
				pipeline = getPipeline();
			}
			String instance = (String)input.get("_instance");
			if (instance == null) {
				instance = getInstance();
			}
			RemusInstance inst = RemusInstance.getInstance(datasource, pipeline, instance);
			if (input.containsKey("_keys")) {
				return new AppletInput(pipeline, 
						inst,
						(String)input.get("_applet"), 
						(List)input.get("_keys") );
			} else {
				return new AppletInput(pipeline, inst, (String)input.get("_applet"), null );
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public Object getBase() {
		return base;
	}

	public WorkGenerator getWorkGenerator() {
		try {
			int mode = getMode();
			if (mode == AppletInstanceRecord.MAPPER) {
				return new MapGenerator();
			} else if (mode == AppletInstanceRecord.REDUCER) {	
				return new ReduceGenerator();	
			} else if (mode == AppletInstanceRecord.SPLITTER) {	
				return new SplitGenerator();	
			} else if (mode == AppletInstanceRecord.MERGER) {	
				return new MergeGenerator();	
			} else if (mode == AppletInstanceRecord.MATCHER) {	
				return new MatchGenerator();	
			} else if (mode == AppletInstanceRecord.PIPE) {	
				return new PipeGenerator();	
			} else if (mode == AppletInstanceRecord.AGENT) {	
				return new AgentGenerator();	
			} else if (mode == AppletInstanceRecord.REMAPPER) {
				return new ReMapGenerator();
			} else if (mode == AppletInstanceRecord.REREDUCER) {
				return new ReReduceGenerator();
			} 
		} catch (RemusDatabaseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public int getAxis() {
		Map inputInfo = (Map) base.get("_input");
		String axis = (String)inputInfo.get(PipelineSubmission.AXIS_FIELD);
		if (axis.compareTo("_left") == 0) {
			return PipelineSubmission.LEFT_AXIS;
		}
		if (axis.compareTo("_right") == 0) {
			return PipelineSubmission.RIGHT_AXIS;
		}
		return 0;
	}

	@Override
	public String toJSONString() {
		return JSON.dumps(base);
	}

	@Override
	public String toString() {
		return base.toString();
	}


}
