package org.remus.work;

import org.apache.thrift.TException;
import org.remus.RemusInstance;
import org.remus.manage.WorkStatusImpl;
import org.remus.server.DataStackRef;
import org.remusNet.thrift.AppletRef;

public class MergeGenerator implements WorkGenerator {
	@Override
	public void writeWorkTable(RemusAppletImpl applet,
			RemusInstance instance) {
		try {
			int jobID = 0;
			AppletRef ar = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() );
			AppletRef arWork = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() + "/@work" );

			DataStackRef lRef = DataStackRef.fromSubmission(applet, applet.getLeftInput(), instance );
			DataStackRef rRef = DataStackRef.fromSubmission(applet, applet.getRightInput(), instance );
			for (String key : lRef.listKeys(applet.datastore)) {
				applet.datastore.add( arWork, 0, 0, Integer.toString(jobID), key );
				jobID++;							
			}

			long t = applet.datastore.getTimeStamp( ar );
			WorkStatusImpl.setWorkStat( applet, instance, 0,0,0, jobID, t);
		}catch (TException e) {
			e.printStackTrace();
		}
	}

}
