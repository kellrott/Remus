package org.remus.work;


import org.remus.RemusInstance;
import org.remus.manage.WorkStatusImpl;

public class SplitGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(RemusAppletImpl applet, RemusInstance instance) {
		applet.datastore.add( applet.getPath() + "/@work", instance.toString(), 0,0, "0", instance.toString() );
		long t = applet.datastore.getTimeStamp(applet.getPath() + "/@done", instance.toString() );
		WorkStatusImpl.setWorkStat( applet, instance, 0, 0, 0, 1, t);
	}

}
