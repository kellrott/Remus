package org.remus;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

import org.mpstore.MPStore;
import org.remus.applet.RemusApplet;

public class RemusApp {
	public static final String configStore = "org.remus.mpstore";
	public static final String configSource = "org.remus.srcdir";
	public static final String configWork = "org.remus.workdir";

	File srcbase;
	MPStore workStore;
	CodeManager codeManager;
	public String baseURL = "";
	
	public RemusApp( File srcdir, MPStore workStore ) throws RemusDatabaseException {
		this.srcbase = srcdir;
		this.workStore = workStore;
		codeManager = new CodeManager(this);
		scanSource(srcbase);
		codeManager.mapPipelines();
		codeManager.startWorkQueue();
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
				for ( RemusApplet code : p.parse(fis, pagePath) ) {
					codeManager.put(code.getPath(), code);
				}
			} catch (FileNotFoundException e) {

			}
		}		
		if ( curFile.isDirectory() ) {
			for ( File child : curFile.listFiles() ) {
				scanSource(child);
			}
		}
	}

	public File getSrcBase() {
		return srcbase;
	}


	public Map<String, PluginConfig> getPluginMap() {
		// TODO Auto-generated method stub
		return null;
	}

	public MPStore getDataStore() {
		return workStore;
	}	

}
