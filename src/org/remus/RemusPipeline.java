package org.remus;

import java.util.LinkedList;
import java.util.List;

public class RemusPipeline {

	List<RemusApplet> members;
	public RemusPipeline(CodeManager parent) {
		members = new LinkedList<RemusApplet>();
	}
	
	public void addApplet(RemusApplet applet) {
		members.add(applet);
	}
	
	
}
