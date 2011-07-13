package org.remus.work;


import org.remus.RemusInstance;
import org.remus.manage.WorkStatus;

public class AgentGenerator  implements WorkGenerator {
	RemusApplet applet;
	RemusInstance inst;
	boolean done;
	@Override
	public void writeWorkTable(RemusApplet applet, RemusInstance instance) {
		done = false;
		this.applet = applet;
		this.inst = instance;
		int jobID = 0;
		System.out.println("AGENT WORK");
		for ( String input : applet.getInputs() ) {			
			String key = instance.toString() + ":" + input;
			applet.datastore.add(applet.getPath() + "/@work", instance.toString(), 0, 0, Integer.toString(jobID), key);
			jobID++;							
		}
		long t = applet.datastore.getTimeStamp(applet.getPath(), instance.toString() );
		WorkStatus.setWorkStat( applet, instance, 0, 0, 0, jobID, t);
	}
}
