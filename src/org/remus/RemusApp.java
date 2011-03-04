package org.remus;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpstore.JsonSerializer;
import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.mpstore.Serializer;
import org.remus.work.AppletInstance;
import org.remus.work.RemusApplet;
import org.remus.work.WorkKey;

public class RemusApp {
	public static final String configStore = "org.remus.mpstore";
	public static final String configWork = "org.remus.workdir";

	//File srcbase;
	public String baseURL = "";
	Map<String,RemusPipeline> pipelines;
	Map params;
	public RemusApp( Map params ) throws RemusDatabaseException {
		this.params = params;
		//scanSource(srcbase);
		loadPipelines();
	}

	public void setBaseURL(String baseURL) {
		this.baseURL = baseURL;
	}

	public void loadPipelines() {
		try { 
			pipelines = new HashMap<String, RemusPipeline>();
			String mpStore = (String)params.get(RemusApp.configStore);
			Class<?> mpClass = Class.forName(mpStore);			
			MPStore store = (MPStore) mpClass.newInstance();
			Serializer serializer = new JsonSerializer();
			store.init(serializer, params);			

			for ( KeyValuePair kv : store.listKeyPairs("/@pipeline", RemusInstance.STATIC_INSTANCE_STR ) ) {
				loadPipeline(kv.getKey(), store, serializer);
			}
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

	public void loadPipeline(String name, MPStore store, Serializer serializer) {
		RemusPipeline pipeline = new RemusPipeline(name, store);		
		for ( KeyValuePair kv : store.listKeyPairs( "/" + name, RemusInstance.STATIC_INSTANCE_STR) ) {
			RemusApplet applet = loadApplet( name, kv.getKey(), store, serializer );
			pipeline.addApplet(applet);
		}
		pipelines.put(pipeline.id, pipeline);	
	}

	/*
	void scanSource(File curFile) {
		if ( curFile.isFile() && curFile.getName().endsWith( ".xml" ) ) {
			try { 
				FileInputStream fis = new FileInputStream(curFile);
				String pagePath = curFile.getAbsolutePath().replaceFirst( "^" + srcbase.getAbsolutePath(), "" ).replaceFirst(".xml$", "");
				RemusParser p = new RemusParser(this);
				List<RemusApplet> appletList = p.parse(fis, pagePath);

				String mpStore = (String)params.get(RemusApp.configStore);
				Class<?> mpClass = Class.forName(mpStore);			
				MPStore store = (MPStore) mpClass.newInstance();
				Serializer serializer = new JsonSerializer();
				store.init(serializer, params);			

				RemusPipeline pipeline = new RemusPipeline(p.getPipelineName(), store);
				for ( RemusApplet applet : appletList ) {
					pipeline.addApplet(applet);
				}
				pipelines.put(pipeline.id, pipeline);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileNotFoundException e) {

			}
		}		
		if ( curFile.isDirectory() ) {
			for ( File child : curFile.listFiles() ) {
				scanSource(child);
			}
		}
	}
	 */

	private RemusApplet loadApplet(String pipelineName, String name, MPStore store, Serializer serializer) {
		String dbPath = "/" + pipelineName + ":" + name;
		String code = null;
		for ( Object obj : store.get(dbPath, RemusInstance.STATIC_INSTANCE_STR, "code") ) {
			code = (String)obj;
		}
		String type = null;
		for ( Object obj : store.get(dbPath, RemusInstance.STATIC_INSTANCE_STR, "type") ) {
			type = (String)obj;
		}
		String codeType = null;
		for ( Object obj : store.get(dbPath, RemusInstance.STATIC_INSTANCE_STR, "codeType") ) {
			codeType = (String)obj;
		}

		CodeFragment cf =  new CodeFragment(codeType, code);
		int appletType = RemusApplet.MAPPER;
		if ( type.compareTo("map") == 0 ) {
			appletType = RemusApplet.MAPPER;
		}
		if ( type.compareTo("reduce") == 0 ) {
			appletType = RemusApplet.REDUCER;
		}
		if ( type.compareTo("pipe") == 0 ) {
			appletType = RemusApplet.PIPE;
		}
		if ( type.compareTo("merge") == 0 ) {
			appletType = RemusApplet.MERGER;
		}
		if ( type.compareTo("match") == 0 ) {
			appletType = RemusApplet.MATCHER;
		}
		RemusApplet applet = RemusApplet.newApplet(name, cf, appletType);
		
		if ( appletType == RemusApplet.MATCHER || appletType == RemusApplet.MERGER ) {
			for ( Object input : store.get( dbPath, RemusInstance.STATIC_INSTANCE_STR, "leftInput" ) ) {
				try {
					RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
					applet.addLeftInput(path);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for ( Object input : store.get( dbPath, RemusInstance.STATIC_INSTANCE_STR, "rightInput" ) ) {
				try {
					RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
					applet.addRightInput(path);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}		
		} else {
			for ( Object input : store.get( dbPath, RemusInstance.STATIC_INSTANCE_STR, "input" ) ) {
				try {
					RemusPath path = new RemusPath( this, (String)input, pipelineName, name );
					applet.addInput(path);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return applet;
	}

	public void addPipeline( RemusPipeline rp ) {
		pipelines.put(rp.id, rp);
	}


	public Map<String, PluginConfig> getPluginMap() {
		// TODO Auto-generated method stub
		return null;
	}

	public Map<AppletInstance,Set<WorkKey>> getWorkQueue(int maxSize) {		
		Map<AppletInstance,Set<WorkKey>> out = new HashMap<AppletInstance,Set<WorkKey>>();
		for ( RemusPipeline pipeline : pipelines.values() ) {			
			if ( out.size() < maxSize ) {
				out.putAll( pipeline.getWorkQueue( maxSize - out.size() ) );
			}
		}
		return out;		
	}

	public RemusApplet getApplet(String appletPath) {
		if (appletPath.startsWith("/"))
			appletPath = appletPath.replaceFirst("^\\/", "");
		String []tmp = appletPath.split(":");
		return pipelines.get(tmp[0]).getApplet(tmp[1]);
	}

	public boolean hasApplet(String appletPath) {
		if (appletPath.startsWith("/"))
			appletPath = appletPath.replaceFirst("^\\/", "");
		String []tmp = appletPath.split(":");
		if ( pipelines.containsKey(tmp[0]) && pipelines.get(tmp[0]).hasApplet( tmp[1] ) )		
			return true;
		return false;
	}

	/*
	public void kickStart() {
		try {
			codeManager.startWorkQueue();
		} catch (RemusDatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	 */
}
