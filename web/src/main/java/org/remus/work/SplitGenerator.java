package org.remus.work;


import org.apache.thrift.TException;
import org.remus.RemusInstance;
import org.remus.manage.WorkStatusImpl;
import org.remusNet.thrift.AppletRef;

public class SplitGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(RemusAppletImpl applet, RemusInstance instance) {
		try {
			AppletRef ar = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() );
			AppletRef arWork = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() + "/@work" );
			applet.datastore.add( arWork, 0,0, "0", instance.toString() );
			long t = applet.datastore.getTimeStamp( ar );
			WorkStatusImpl.setWorkStat( applet, instance, 0, 0, 0, 1, t);
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}
