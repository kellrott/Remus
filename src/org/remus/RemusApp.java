package org.remus;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpstore.JsonSerializer;
import org.mpstore.MPStore;
import org.mpstore.Serializer;
import org.remus.work.AppletInstance;
import org.remus.work.RemusApplet;
import org.remus.work.WorkKey;

public class RemusApp {
	public static final String configStore = "org.remus.mpstore";
	public static final String configSource = "org.remus.srcdir";
	public static final String configWork = "org.remus.workdir";

	File srcbase;
	public String baseURL = "";
	Map<String,RemusPipeline> pipelines;
	Map params;
	public RemusApp( File srcdir, Map params ) throws RemusDatabaseException {
		this.srcbase = srcdir;
		pipelines = new HashMap<String, RemusPipeline>();
		this.params = params;
		scanSource(srcbase);
		//codeManager.mapPipelines();
	}

	public void setBaseURL(String baseURL) {
		this.baseURL = baseURL;
	}

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


	public void addPipeline( RemusPipeline rp ) {
		pipelines.put(rp.id, rp);
	}

	public File getSrcBase() {
		return srcbase;
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
