package org.remus.core;

import java.util.List;

import org.remus.RemusDB;
import org.remus.thrift.AppletRef;

public class AppletInput {

	String pipeline;
	RemusInstance instance;
	String applet;
	List<String> keys;

	public AppletInput(String pipeline, RemusInstance instance, String applet, List<String> keys) {
		this.pipeline = pipeline;
		this.instance = instance;
		this.applet = applet;
		this.keys = keys;
	}

	@Override
	public String toString() {
		return this.pipeline + ":" + this.instance + ":" + this.applet;
	}
	
	public Iterable<String> listKeys(RemusDB datastore) {
		if (keys != null) {
			return keys;
		}
		AppletRef iref = new AppletRef(pipeline, instance.toString(), applet);
		return datastore.listKeys(iref);		
	}

	public String getPipeline() {
		return pipeline;
	}

	public RemusInstance getInstance() {
		return instance;
	}

	public String getApplet() {
		return applet;
	}

}
