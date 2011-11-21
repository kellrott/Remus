package org.remus.work;


import org.apache.thrift.TException;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.core.AppletInstance;
import org.remus.core.AppletInstanceRecord;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;

public class SplitGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(AppletInstanceRecord air, RemusDB datastore, RemusAttach attachstore) {
		try {
			AppletRef ar = new AppletRef(air.getPipeline(), air.getInstance(), air.getApplet());
			AppletRef arWork = new AppletRef(air.getPipeline(), air.getInstance(), air.getApplet() + Constants.WORK_APPLET);
			datastore.add(arWork, 0, 0, "0", air.getInstance());
			long t = datastore.getTimeStamp(ar);
			AppletInstance ai = new AppletInstance(air, datastore, attachstore);
			ai.setWorkStat(0, 0, 0, 1, t);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void finalizeWork(AppletInstanceRecord air, RemusDB datastore) {
		// TODO Auto-generated method stub
		
	}
}
