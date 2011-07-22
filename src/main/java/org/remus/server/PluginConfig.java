package org.remus.server;

import java.util.Map;

public class PluginConfig {
	public PluginConfig(String string) {
		classPath = string;
	}

	public Map<String,String> param;
	public String classPath;
	
	public String get(String name) {
		return param.get(name);
	}
}
