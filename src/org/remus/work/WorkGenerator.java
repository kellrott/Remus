package org.remus.work;

import java.util.Set;

import org.remus.RemusInstance;

public interface WorkGenerator {
	public Set<WorkKey> getActiveKeys(RemusApplet applet, RemusInstance instance, long reqCount);
	public AppletInstance getAppletInstance();
	boolean isDone();
}
