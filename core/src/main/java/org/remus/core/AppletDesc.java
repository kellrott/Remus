package org.remus.core;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AppletDesc {

	Map base;

	public AppletDesc(Map applet) {
		base = applet;
	}

	public List<String> getAttachments() {
		List<String> out = new LinkedList<String>();
		if (base.containsKey("_include")) {
			for (Object name : (List) base.get("_include")) {
				out.add((String) name);
			}
		}
		String code = (String) base.get("_code");
		if (code != null) {
			out.add(code);
		}
		return out;
	}

}
