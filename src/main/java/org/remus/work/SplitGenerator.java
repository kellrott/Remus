package org.remus.work;


import org.remus.RemusInstance;
import org.remus.manage.WorkStatus;

public class SplitGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(RemusApplet applet, RemusInstance instance) {
		applet.datastore.add( applet.getPath() + "/@work", instance.toString(), 0,0, "0", instance.toString() );
		long t = applet.datastore.getTimeStamp(applet.getPath() + "/@done", instance.toString() );
		WorkStatus.setWorkStat( applet, instance, 0, 0, 0, 1, t);
	}

}
