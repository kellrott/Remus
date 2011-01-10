package org.semweb.app;

import java.io.File;

public class CacheManager {
	File basedir;
	File extdir;
	
	public CacheManager(File basedir){
		if ( !basedir.exists()) {
			basedir.mkdir();
		}
		this.basedir = basedir;
		this.extdir = new File( basedir, "external" );
		if ( !extdir.exists() ) {
			extdir.mkdir();
		}
	}
	
	
}
