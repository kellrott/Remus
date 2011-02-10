package org.remus;


import org.remus.applet.RemusApplet;


public class WorkDescription {
	public Object desc=null;
	public long jobID;
	public RemusApplet applet;
	public RemusInstance instance;
	public WorkDescription(RemusApplet applet, RemusInstance instance, long jobID, Object desc) {
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
