package org.remus.work;


import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.core.WorkStatus;
import org.remus.thrift.AppletRef;

public class SplitGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(RemusPipeline pipeline, RemusApplet applet, RemusInstance instance, RemusDB datastore) {
		try {
			AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() );
			AppletRef arWork = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + "/@work" );
			datastore.add( arWork, 0,0, "0", instance.toString() );
			long t = datastore.getTimeStamp( ar );
			WorkStatus.setWorkStat( pipeline, applet, instance, 0, 0, 0, 1, t);
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}
