package org.semweb.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.semweb.app.SemWebApp;
import org.semweb.plugins.ExtInterface;


public class ExtManager {
	Map<String, ExtInterface> dsList;

	public ExtManager(SemWebApp app) {

		Map<String, ExtConfig> exts = app.getExtMap();
		dsList = new HashMap<String, ExtInterface>();

		for ( String plugName : exts.keySet() ) {
			try {
				ExtConfig config = exts.get(plugName);
				//System.out.println("LOADING: " + config.classPath );
				System.out.flush();
				Class<?> c = Class.forName(config.classPath);
				ExtInterface ds = (ExtInterface) c.newInstance();
				dsList.put( plugName , ds);
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
	
	public Set<String>  getDataSourceNames() {
		return dsList.keySet();
	}

	public ExtInterface getDataSource(String name) {
		return dsList.get(name);
	}

	public void setDataSource(String name, ExtInterface ds) {
		dsList.put(name, ds);
	}

}
