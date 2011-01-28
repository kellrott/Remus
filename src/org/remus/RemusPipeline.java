package org.remus;

import java.util.LinkedList;
import java.util.List;

public class RemusPipeline {

	boolean dynamic = false;
	List<RemusApplet> members;
	public RemusPipeline(CodeManager parent) {
		members = new LinkedList<RemusApplet>();
	}

	public void addApplet(RemusApplet applet) {
		for ( InputReference iref : applet.getInputs() ) {
			if ( iref.dynamicInput ) {
				dynamic = true;
			}
		}
		members.add(applet);
	}


}
