package org.remus.work;

import java.util.Set;

import org.remus.RemusInstance;

public abstract class AppletInstance {
	public RemusInstance inst;
	public RemusApplet applet;	
	AppletInstance( RemusApplet applet, RemusInstance inst) {
		this.applet = applet;
		this.inst = inst;
	}	
	abstract public  Object formatWork(Set<WorkKey> keys);


	@Override
	public int hashCode() {
		return applet.hashCode() + inst.hashCode();
	}


	@Override
	public boolean equals(Object obj) {
		AppletInstance a = (AppletInstance)obj;
		if ( a.applet.equals(applet) && a.inst.equals(inst) )
			return true;
		return false;
	}
	
	@Override
	public String toString() {
		return inst.toString() + applet.getPath();
	}
}
