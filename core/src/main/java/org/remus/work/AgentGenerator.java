package org.remus.work;


import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.core.AppletInstance;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class AgentGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(RemusPipeline pipeline, RemusApplet applet, RemusInstance instance, RemusDB datastore) {

		AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), applet.getID());
		AppletRef arWork = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + "/@work");
		
		int jobID = 0;
		for (String input : applet.getInputs()) {			
			String key = instance.toString() + ":" + input;
			try {
				datastore.add(arWork, 0, 0, Integer.toString(jobID), key);
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			jobID++;							
		}
		try {
			long t = datastore.getTimeStamp(ar);
			AppletInstance ai = new AppletInstance(pipeline, instance, applet, datastore);
			ai.setWorkStat(0, 0, 0, jobID, t);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
}
