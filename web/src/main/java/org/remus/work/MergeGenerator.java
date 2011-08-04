package org.remus.work;

import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.core.WorkStatus;
import org.remus.server.DataStackRef;
import org.remus.thrift.AppletRef;

public class MergeGenerator implements WorkGenerator {
	@Override
	public void writeWorkTable(RemusPipeline pipeline, RemusApplet applet, RemusInstance instance, RemusDB datastore) {

		try {
			int jobID = 0;
			AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() );
			AppletRef arWork = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + "/@work" );

			DataStackRef lRef = DataStackRef.fromSubmission(pipeline, applet, applet.getLeftInput(), instance );
			DataStackRef rRef = DataStackRef.fromSubmission(pipeline, applet, applet.getRightInput(), instance );
			for (String key : lRef.listKeys(datastore)) {
				datastore.add( arWork, 0, 0, Integer.toString(jobID), key );
				jobID++;							
			}

			long t = datastore.getTimeStamp( ar );
			WorkStatus.setWorkStat( pipeline, applet, instance, 0,0,0, jobID, t);
		}catch (TException e) {
			e.printStackTrace();
		}
	}

}
