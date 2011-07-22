package org.remus.work;


import org.remus.RemusInstance;
import org.remus.manage.WorkStatusImpl;
import org.remus.server.DataStackRef;

public class MatchGenerator implements WorkGenerator {
	@Override
	public void writeWorkTable(RemusAppletImpl applet, RemusInstance instance) {
		int jobID = 0;
		DataStackRef lRef = DataStackRef.fromSubmission(applet, applet.getLeftInput(), instance );
		DataStackRef rRef = DataStackRef.fromSubmission(applet, applet.getRightInput(), instance );
		for ( String key : lRef.listKeys( applet.datastore ) ) {
			applet.datastore.add( applet.getPath() + "/@work", instance.toString(), 0, 0, Integer.toString(jobID), key );
			jobID++;							
		}

		long t = applet.datastore.getTimeStamp(applet.getPath(), instance.toString() );
		WorkStatusImpl.setWorkStat( applet, instance, 0, 0, 0, jobID, t);
	}
}
