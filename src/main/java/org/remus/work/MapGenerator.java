package org.remus.work;


import org.remus.DataStackRef;
import org.remus.RemusInstance;
import org.remus.manage.WorkStatus;

public class MapGenerator implements WorkGenerator {
	@Override
	public void writeWorkTable(RemusApplet applet, RemusInstance instance) {
		DataStackRef iRef = DataStackRef.fromSubmission( applet, applet.getInput(), instance );
		int jobID = 0;		
		for ( String key : iRef.listKeys( applet.datastore ) ) {
			applet.datastore.add( applet.getPath() + "/@work", instance.toString(), 0,0, Integer.toString(jobID), key );
			jobID++;							
		}		
		long t = applet.datastore.getTimeStamp(applet.getPath(), instance.toString() );
		WorkStatus.setWorkStat( applet, instance, 0, 0, 0, jobID, t);
	}
}
