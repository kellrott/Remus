package org.remus.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONAware;
import org.json.simple.JSONValue;

public class PipelineDesc implements JSONAware {

	Map base;
	public PipelineDesc(Object base) {
		this.base = (Map) base;
	}
	
	@Override
	public String toJSONString() {
		return JSONValue.toJSONString(base);
	}

	public List<String> getApplets() {	
		List<String> out = new LinkedList<String>();
		for (Object key : base.keySet()) {
			Object obj = base.get(key);
			if (obj instanceof Map) {
				if (((Map) obj).containsKey("_mode")) {
					out.add((String) key);
					if (((Map) obj).containsKey("_output")) {
						List output = (List) ((Map) obj).get("_output");
						for (Object name : output) {
							out.add((String) key + ":" + (String) name);
						}
					}
				}
			}
		}
		return out;
	}

	public Map getApplet(String appletName) {
		if (appletName.contains(":")) {
			Map info = new HashMap();
			info.put("_mode", "output");
			return info;
		} 
		return (Map) base.get(appletName);
	}

	public List<String> getAttachments() {
		List<String> out = new LinkedList<String>();
		if (base.containsKey("_include")) {
			for (Object name : (List) base.get("_include")) {
				out.add((String) name);
			}
		}
		for (String appletName : getApplets()) {
			String code = (String) getApplet(appletName).get("_code");
			if (code != null && !code.startsWith(":")) {
				out.add(code);
			}
		}
		return out;
	}

}
