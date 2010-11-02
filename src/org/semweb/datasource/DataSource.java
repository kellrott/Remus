package org.semweb.datasource;

import java.util.Map;

import org.semweb.PageInterface;
import org.semweb.config.ConfigMap;

public abstract class DataSource {

	
	public abstract void init( ConfigMap configMap ) throws InitException;
	
	public void register() {

	}
	
	
}
