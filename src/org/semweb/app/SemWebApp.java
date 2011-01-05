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
import org.semweb.config.ExtConfig;
import org.semweb.config.ScriptingConfig;


public class SemWebApp {

	File appBase;
	File configDir;
	Map <String,ExtConfig> extMap;
	Map <String,ScriptingConfig> scriptingMap;
	public String templatingClass;
	PageManager pageRender;
	public SemWebApp(File base) {
		
		appBase = base;

		configDir = new File(base, ".semweb");
		if ( !configDir.exists() ) {
			createConfig();
		}	
		try {
			JSONTokener jt = new JSONTokener( new InputStreamReader( new FileInputStream(new File(configDir, "config"))));
			JSONObject js = new JSONObject(jt);
			//System.out.println( js );
			extMap = new HashMap<String, ExtConfig>();
			if ( js.has("plugins") ) {
				JSONObject pluginList = js.getJSONObject("plugins");
				Iterator i = pluginList.keys();
				while ( i.hasNext() ) {
					String name = (String)i.next();
					ExtConfig config =  new ExtConfig( pluginList.getJSONObject(name).getString("class") ) ;
					config.param = new HashMap<String, String>();
					JSONObject paramList = pluginList.getJSONObject(name).getJSONObject("param"); 
					Iterator i2 = paramList.keys();
					while ( i2.hasNext() ) {
						String key = (String)i2.next();
						config.param.put(key, paramList.getString(key));
					}
					extMap.put(name, config );
				}
			}

			scriptingMap = new HashMap<String, ScriptingConfig>();
			if ( js.has("scripting") ) {
				JSONObject scriptingList = js.getJSONObject("scripting");
				Iterator i = scriptingList.keys();
				while ( i.hasNext() ) {
					String name = (String)i.next();
					String classPath = scriptingList.getJSONObject(name).getString("class");
					ScriptingConfig config = new ScriptingConfig( classPath );
					scriptingMap.put(name, config );
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
		pageRender = new PageManager(this);
	}

	public InputStream readPage(String path ) {
		PageRequest page = pageRender.openPage(path, null);
		if (page == null)
			return null;
		InputStream is = page.open();
		return is;
	}

	public void createConfig() {
		configDir.mkdir();
		File configFile = new File( configDir, "config" );
		try {
			FileOutputStream fos = new FileOutputStream(configFile);
			fos.write("{}".getBytes());
			configFile.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Map<String, ExtConfig> getExtMap() {
		return extMap;
	}

	public Map<String, ScriptingConfig> getScriptingMap() {
		return scriptingMap;
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


}
