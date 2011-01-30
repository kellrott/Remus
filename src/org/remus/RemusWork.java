package org.remus;

import org.remus.applet.RemusApplet;

public class RemusWork {
	RemusPipeline parent;
	RemusApplet applet;
	RemusInstance instance;
	int jobID;

	RemusWork(RemusPipeline parent, RemusApplet applet, RemusInstance instance, int jobID) {
		this.parent = parent;
		this.jobID = jobID;
		this.applet = applet;
		this.instance = instance;
	}


	@Override
	public String toString() {
		return parent + "=" + applet.getPath() + "=" + jobID;
	}
}
