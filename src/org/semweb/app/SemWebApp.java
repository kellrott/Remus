package org.semweb.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.*;
import org.semweb.config.PluginConfig;
import org.semweb.config.SemwebConfig;


public class SemWebApp {

	File appBase;
	File configDir, configFile;
	Map <String,PluginConfig> pluginMap;
	public String templatingClass;
	PageManager pageMan;
	public SemWebApp(File base) {

		appBase = base;

		configDir = new File(base, ".semweb");
		configFile = new File(configDir, "config");
		if ( !configDir.exists()  || !configFile.exists()) {
			createConfig();
		}	
		try {
			JSONTokener jt = new JSONTokener( new InputStreamReader( new FileInputStream(configFile)));
			JSONObject js = new JSONObject(jt);
			//System.out.println( js );
			pluginMap = new HashMap<String, PluginConfig>();
			if ( js.has("plugins") ) {
				JSONObject pluginList = js.getJSONObject("plugins");
				Iterator i = pluginList.keys();
				while ( i.hasNext() ) {
					String name = (String)i.next();
					PluginConfig config =  new PluginConfig( pluginList.getJSONObject(name).getString("class") ) ;
					config.param = new HashMap<String, String>();
					if ( pluginList.getJSONObject(name).has("param") ) {
						JSONObject paramList = pluginList.getJSONObject(name).getJSONObject("param"); 
						Iterator i2 = paramList.keys();
						while ( i2.hasNext() ) {
							String key = (String)i2.next();
							config.param.put(key, paramList.getString(key));
						}
					}
					pluginMap.put(name, config );
				}
			}


			if ( js.has("templating") ) {
				JSONObject templateInfo = js.getJSONObject("templating");
				templatingClass = templateInfo.getString("class");				
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		pageMan = new PageManager(this);
	}

	public InputStream readPage(String path ) {
		PageRequest page = pageMan.openPage(path);
		if (page == null)
			return null;
		InputStream is = page.open();
		return is;
	}

	public void createConfig() {
		configDir.mkdir();
		File configFile = new File( configDir, "config" );
		try {
			InputStream is = SemwebConfig.class.getResourceAsStream("default.config");
			FileOutputStream fos = new FileOutputStream(configFile);
			byte [] buffer = new byte[2000];
			int len;
			while ( (len=is.read(buffer)) > 0 ) {
				fos.write(buffer, 0, len);
			}
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Map<String, PluginConfig> getPluginMap() {
		return pluginMap;
	}




	public static void main(String [] args) {
		SemWebApp app = new SemWebApp( new File(args[0]) );
		InputStream is = app.readPage( args[1] );
		try {
			int i;
			while ( (i = is.read()) != -1 ) {
				System.out.print( (char)i ) ;
			}
		} catch ( IOException e ) {

		}
	}

	public File getPageBase() {
		return appBase;
	}

	public PageManager getPageManager() {
		return pageMan;
	}


}
