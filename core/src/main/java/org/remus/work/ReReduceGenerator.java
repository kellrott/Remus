package org.remus.work;

import org.apache.thrift.TException;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.core.AppletInput;
import org.remus.core.AppletInstance;
import org.remus.core.AppletInstanceRecord;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;

public class ReReduceGenerator implements WorkGenerator {

	@Override
	public void finalizeWork(AppletInstanceRecord air, RemusDB datastore) {
		
	}

	@Override
	public void writeWorkTable(AppletInstanceRecord air, RemusDB datastore, RemusAttach attachstore) {
		try {
			AppletRef ar = new AppletRef(air.getPipeline(), air.getInstance(), air.getApplet());
			AppletRef arWork = new AppletRef(air.getPipeline(), air.getInstance(), air.getApplet() + Constants.WORK_APPLET);
			AppletInput iRef = air.getInput(air.getSource(), datastore);
			int jobID = 0;

			for (String key : iRef.listKeys(datastore)) {
				datastore.add(arWork, 0, 0, Integer.toString(jobID), key);
				jobID++;
			}
			long t = datastore.getTimeStamp(ar);
			AppletInstance ai = new AppletInstance(air, datastore, attachstore);
			ai.setWorkStat(0, 0, 0, jobID, t);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
