package org.remus.applet;

import java.util.Map;

import org.remus.RemusInstance;
import org.remus.WorkDescription;

public interface WorkGenerator {
	public void init(RemusApplet applet);
	public void startWork(RemusInstance instance);
	public WorkDescription nextWork();	
	public Map getDescMap();
}
