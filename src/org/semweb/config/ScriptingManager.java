package org.semweb.config;

import java.util.HashMap;
import java.util.Map;

import org.semweb.app.SemWebApp;
import org.semweb.scripting.ScriptingInterface;

public class ScriptingManager {
	Map<String,ScriptingInterface> seList;
	public ScriptingManager(SemWebApp app) {

		Map<String, ScriptingConfig> exts = app.getScriptingMap();
		seList = new HashMap<String, ScriptingInterface>();

		for ( String plugName : exts.keySet() ) {
			try {
				ScriptingConfig scConfig = exts.get(plugName);
				//System.out.println("LOADING: " + scConfig.classPath );
				System.out.flush();
				Class<?> c = Class.forName(scConfig.classPath);
				ScriptingInterface ds = (ScriptingInterface) c.newInstance();
				ds.init(scConfig);
				seList.put( plugName , ds);
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
	
	public boolean hasLang(String lang) {
		return seList.containsKey(lang);
	}
	
	public ScriptingInterface getLang(String lang ) {
		return seList.get(lang);
	}
}
