package org.remus.work;

import org.apache.thrift.TException;
import org.remus.RemusInstance;
import org.remus.manage.WorkStatusImpl;
import org.remus.server.DataStackRef;
import org.remusNet.thrift.AppletRef;

public class ReduceGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(RemusAppletImpl applet,
			RemusInstance instance) {
		try {
			AppletRef ar = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() );
			AppletRef arWork = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() + "/@work" );

			DataStackRef iRef = DataStackRef.fromSubmission(applet, applet.getInput(), instance);
			int jobID = 0;

			for ( String key : iRef.listKeys( applet.datastore  ) ) {
				applet.datastore.add( arWork, 0,0, Integer.toString(jobID), key );
				jobID++;
			}
			long t = applet.datastore.getTimeStamp( ar );
			WorkStatusImpl.setWorkStat( applet, instance, 0, 0, 0, jobID, t);
		}catch (TException e ) {
			e.printStackTrace();
		}
	}

}
