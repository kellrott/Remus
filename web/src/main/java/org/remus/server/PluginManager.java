package org.remus.server;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.remus.mapred.InterfaceBase;


public class PluginManager {
	Map<String, InterfaceBase> pluginList;

	public PluginManager(RemusApp app) {

		Map<String, PluginConfig> exts = app.getPluginMap();
		pluginList = new HashMap<String, InterfaceBase>();

		for ( String plugName : exts.keySet() ) {
			try {
				PluginConfig config = exts.get(plugName);
				//System.out.println("LOADING: " + config.classPath );
				System.out.flush();
				Class<?> c = Class.forName(config.classPath);
				InterfaceBase ds = (InterfaceBase) c.newInstance();
				ds.init(config);
				pluginList.put( plugName , ds);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}
	
	public InterfaceBase getPlugin(String name) {
		return pluginList.get(name);
	}

	public boolean hasPlugin(String name) {
		return pluginList.containsKey(name);
	}
}
