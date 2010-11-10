package org.semweb.datasource;


import org.semweb.config.ConfigMap;

public abstract class DataSource {

	public DataSource() { }
	
	public abstract void init( ConfigMap configMap ) throws InitException;
	
	public void register() {

	}
	
	
}
