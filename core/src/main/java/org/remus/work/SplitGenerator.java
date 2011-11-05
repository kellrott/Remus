package org.remus.work;


import org.apache.thrift.TException;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.core.AppletInstance;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;

public class SplitGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(RemusPipeline pipeline, RemusApplet applet, RemusInstance instance, RemusDB datastore, RemusAttach attachstore) {
		try {
			AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), applet.getID());
			AppletRef arWork = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + Constants.WORK_APPLET);
			datastore.add(arWork, 0, 0, "0", instance.toString());
			long t = datastore.getTimeStamp(ar);
			AppletInstance ai = new AppletInstance(pipeline, instance, applet, datastore, attachstore);
			ai.setWorkStat(0, 0, 0, 1, t);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void finalizeWork(RemusPipeline pipeline, RemusApplet applet,
			RemusInstance instance, RemusDB datastore) {
		// TODO Auto-generated method stub
		
	}
}
