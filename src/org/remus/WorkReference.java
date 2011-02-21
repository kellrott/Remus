package org.remus;

import org.remus.applet.RemusApplet;

public class WorkReference implements Comparable<WorkReference> {
	public long jobID;
	public RemusApplet applet;
	public RemusInstance instance;
	public WorkReference(RemusApplet applet, RemusInstance instance, long jobID) {
		this.jobID = jobID;
		this.applet = applet;
		this.instance = instance;
	}

	@Override
	public int compareTo(WorkReference arg) {
		int appCompare = applet.getPath().compareTo( arg.applet.getPath() );
		if ( appCompare != 0 )
			return appCompare * 1000;
		int instCompare = instance.compareTo(arg.instance);
		if ( instCompare != 0 )
			return instCompare * 100;
		return (int)(jobID - arg.jobID);
	}
	
	@Override
	public boolean equals(Object obj) {
		WorkReference a = (WorkReference) obj;
		if ( jobID == a.jobID && applet.getPath().compareTo(a.applet.getPath()) == 0 && a.instance.toString().compareTo( a.instance.toString() ) == 0  )
			return true;
		return false;
	}
	
	
	@Override
	public int hashCode() {
		return ((int)jobID) + applet.getPath().hashCode() + instance.id.hashCode();
	}
}
