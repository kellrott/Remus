package org.remus.work;


import org.apache.thrift.TException;
import org.remus.RemusInstance;
import org.remus.manage.WorkStatusImpl;
import org.remusNet.thrift.AppletRef;

public class AgentGenerator  implements WorkGenerator {
	RemusAppletImpl applet;
	RemusInstance inst;
	boolean done;
	@Override
	public void writeWorkTable(RemusAppletImpl applet, RemusInstance instance) {

		AppletRef ar = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() );
		AppletRef arWork = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() + "/@work" );
		
		done = false;
		this.applet = applet;
		this.inst = instance;
		int jobID = 0;
		System.out.println("AGENT WORK");
		for ( String input : applet.getInputs() ) {			
			String key = instance.toString() + ":" + input;
			try {
				applet.datastore.add(ar, 0, 0, Integer.toString(jobID), key);
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			jobID++;							
		}
		try {
			long t = applet.datastore.getTimeStamp( ar );
			WorkStatusImpl.setWorkStat( applet, instance, 0, 0, 0, jobID, t);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
