package org.remus.work;


import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.core.AppletInstance;
import org.remus.core.DataStackRef;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class MatchGenerator implements WorkGenerator {
	@Override
	public void writeWorkTable(RemusPipeline pipeline, RemusApplet applet, RemusInstance instance, RemusDB datastore) {
		try {
			int jobID = 0;
			AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), applet.getID());
			AppletRef arWork = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + "/@work");

			DataStackRef lRef = DataStackRef.fromSubmission(pipeline, applet, applet.getLeftInput(), instance);
			DataStackRef rRef = DataStackRef.fromSubmission(pipeline, applet, applet.getRightInput(), instance);
			for (String key : lRef.listKeys(datastore)) {
				datastore.add(arWork, 0, 0, Integer.toString(jobID), key);
				jobID++;
			}
			long t = datastore.getTimeStamp(ar);
			AppletInstance ai = new AppletInstance(pipeline, instance, applet, datastore);
			ai.setWorkStat(0, 0, 0, jobID, t);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemusDatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
