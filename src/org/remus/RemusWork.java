package org.remus;

public class RemusWork {
	RemusPipeline parent;
	RemusApplet applet;
	int jobID;

	RemusWork(RemusPipeline parent, RemusApplet applet, int jobID) {
		this.parent = parent;
		this.jobID = jobID;
		this.applet = applet;
	}


	@Override
	public String toString() {
		return parent + "=" + applet.getPath() + "=" + jobID;
	}
}
