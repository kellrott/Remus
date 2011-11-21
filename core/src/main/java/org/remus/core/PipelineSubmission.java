package org.remus.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONAware;
import org.json.simple.JSONValue;

public class PipelineSubmission implements JSONAware {

	Map base;
	public static final String SUBMIT_KEY_FIELD = "_submitKey";
	public static final Object WorkDoneField = "_workdone";
	public static final String KeysField = "_keys";
	public static final String INSTANCE_FIELD = "_instance";
	public static final String InputField = "_input";
	public static final String SUBMIT_INIT_FIELD = "_submitInit";
	public static final String SUBMIT_INPUT_FIELD = "_submitInput";
	public static final String AXIS_FIELD = "_axis";
	
	public static final int RIGHT_AXIS = 1;
	public static final int LEFT_AXIS = -1;
	
	public PipelineSubmission(Object subMap) {
		base = (Map) subMap;
	}

	public PipelineSubmission() {
		base = new HashMap();
	}

	@Override
	public String toJSONString() {
		return JSONValue.toJSONString(base);
	}

	public List<String> getInitApplets() {
		List<String> out = new LinkedList<String>();
		if (base.containsKey(PipelineSubmission.SUBMIT_INIT_FIELD)) {
			out = (List) ((Map) base).get(PipelineSubmission.SUBMIT_INIT_FIELD);
		}
		return out;
	}

	public void setSubmitKey(String key) {
		base.put(SUBMIT_KEY_FIELD, key);
	}

	public void setInstance(RemusInstance inst) {
		base.put(INSTANCE_FIELD, inst.toString());		
	}

	public void setInitApplets(List<String> asList) {
		base.put(SUBMIT_INIT_FIELD, asList);		
	}

	public String getInputInstance() {
		Map inputInfo = (Map) base.get("_input");
		return (String) inputInfo.get("_instance");
	}
	
	public String getInputApplet() {
		Map inputInfo = (Map) base.get("_input");
		return (String) inputInfo.get("_applet");
	}

	public String getLeftInputInstance() {
		Map inputInfo = (Map) base.get("_input");
		return (String) ((Map)inputInfo.get("_left")).get("_instance");
	}

	public String getRightInputInstance() {
		Map inputInfo = (Map) base.get("_input");
		return (String) ((Map)inputInfo.get("_right")).get("_instance");
	}

	public String getLeftInputApplet() {
		Map inputInfo = (Map) base.get("_input");
		return (String) ((Map)inputInfo.get("_left")).get("_applet");
	}

	public String getRightInputApplet() {
		Map inputInfo = (Map) base.get("_input");
		return (String) ((Map)inputInfo.get("_right")).get("_applet");
	}

	public int getAxis() {
		Map inputInfo = (Map) base.get("_input");
		String axis = (String)inputInfo.get(AXIS_FIELD);
		if (axis.compareTo("_left") == 0) {
			return LEFT_AXIS;
		}
		if (axis.compareTo("_right") == 0) {
			return RIGHT_AXIS;
		}
		return 0;
	}

	public List getInputList() {
		List inputInfo = (List) base.get("_input");
		return inputInfo;
	}

	public Object get(String key) {
		return base.get(key);
	}

	public RemusInstance getInstance() {
		String instStr = (String) base.get("_instance");
		return new RemusInstance(instStr);
	}

	public void setMode(String mode) {
		base.put("_mode", mode);
	}

	public boolean hasSubmitKey() {
		return base.containsKey(SUBMIT_KEY_FIELD);
	}

	public boolean hasInstance() {
		return base.containsKey(INSTANCE_FIELD);
	}

	public String getSubmitKey() {
		return (String) base.get(SUBMIT_KEY_FIELD);
	}

	public List getSourceList() {
		return (List) base.get("_src");
	}

	public Map getMap() {		
		return base;
	}

	public boolean hasSubmitInput() {
		return base.containsKey(SUBMIT_INPUT_FIELD);
	}

	public boolean hasSubmitInputApplet(String id) {
		return ((Map)base.get(SUBMIT_INPUT_FIELD)).containsKey(id);
	}

	public Map getSubmitInputAppletMap(String id) {
		return (Map) ((Map)base.get(SUBMIT_INPUT_FIELD)).get(id);
	}

	
}
