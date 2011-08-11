package org.remus.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONAware;
import org.json.simple.JSONValue;

public class PipelineSubmission implements JSONAware {

	Map base;
	public static final String SubmitKeyField = "_submitKey";
	public static final Object WorkDoneField = "_workdone";
	public static final String KeysField = "_keys";
	public static final String InstanceField = "_instance";
	public static final String InputField = "_input";
	public static final String AppletField = "_applets";
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
		if (base.containsKey(PipelineSubmission.AppletField)) {
			out = (List) ((Map) base).get(PipelineSubmission.AppletField);
		}
		return out;
	}

	public void setSubmitKey(String key) {
		base.put(SubmitKeyField, key);
	}

	public void setInstance(RemusInstance inst) {
		base.put(InputField, inst.toString());		
	}

	public void setInitApplets(List<String> asList) {
		base.put(AppletField, asList);		
	}

}
