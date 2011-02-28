package org.remus.work;

import java.util.Set;

import org.remus.RemusInstance;

public class SimpleAppletInstance extends AppletInstance {
	public SimpleAppletInstance(RemusApplet applet, RemusInstance inst) {
		super(applet, inst);
	}

	@Override
	public Object formatWork(Set<WorkKey> keys) {	
		return null;
	}

}
