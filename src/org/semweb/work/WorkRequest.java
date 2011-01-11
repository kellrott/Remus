package org.semweb.work;

import org.semweb.app.SemWebApp;
import org.semweb.app.SemWebApplet;

public class WorkRequest {

	
	SemWebApplet applet;
	public WorkRequest( SemWebApp parent, SemWebApplet applet) {
		this.applet = applet;
	}
	
	public SemWebApplet getApplet() {
		return applet;
	}
	
}
