package org.semweb.config;

import java.util.Map;

public class ExtConfig {
	public ExtConfig(String string) {
		classPath = string;
	}

	public Map<String,String> param;
	public String classPath;
	
	public String get(String name) {
		return param.get(name);
	}
}
