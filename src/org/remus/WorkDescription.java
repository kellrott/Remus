package org.remus;

import java.io.File;

import org.remus.applet.RemusApplet;


public class WorkDescription {
	public Object desc=null;
	public int jobID;
	public RemusApplet applet;
	public RemusInstance instance;
	public WorkDescription(RemusApplet applet, RemusInstance instance, int jobID, Object desc) {
		this.jobID = jobID;
		this.desc = desc;
		this.applet = applet;
		this.instance = instance;
	}
	Object getDesc() {
		return desc;
	}
	public Object getInstance() {
		return instance;
	}
	public RemusApplet getApplet() {
		return applet;
	}
}
