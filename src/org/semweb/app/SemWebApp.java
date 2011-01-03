package org.semweb.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
	PageRender pageRender;
	public SemWebApp(File base) {
		/*
		 Repository db = new Repository(new File(file, Constants.DOT_GIT).getAbsolutePath());
		 db.create();
		 db.getConfig().setBoolean("core", null, "bare", false);
		 db.getConfig().save();
		 saveRemote(uri);
		 final FetchResult r = runFetch();
		 final Ref branch = guessHEAD(r);
		 doCheckout(branch);
		 */
		appBase = base;

		configDir = new File(base, ".semweb");
		if ( !configDir.exists() ) {
			createConfig();
		}	
		try {
			JSONTokener jt = new JSONTokener( new InputStreamReader( new FileInputStream(new File(configDir, "config"))));
			JSONObject js = new JSONObject(jt);
			System.out.println( js );

			extMap = new HashMap<String, ExtConfig>();
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


			scriptingMap = new HashMap<String, ScriptingConfig>();
			JSONObject scriptingList = js.getJSONObject("scripting");
			i = scriptingList.keys();
			while ( i.hasNext() ) {
				String name = (String)i.next();
				String classPath = scriptingList.getJSONObject(name).getString("class");
				ScriptingConfig config = new ScriptingConfig( classPath );
				scriptingMap.put(name, config );
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		pageRender = new PageRender(this);
	}

	public InputStream readPage(String path ) {
		return pageRender.openPage(path, null);
	}

	public void createConfig() {
		configDir.mkdir();
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
