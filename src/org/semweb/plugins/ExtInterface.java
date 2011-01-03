package org.semweb.plugins;


import org.semweb.config.ConfigMap;
import org.semweb.config.ExtConfig;

public interface ExtInterface {

	//public ExtIterface();// { }
	
	public abstract void init( ExtConfig config ) throws InitException;
	
	
	
}
