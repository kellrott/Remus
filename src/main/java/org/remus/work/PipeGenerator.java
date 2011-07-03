package org.remus.work;

import java.util.ArrayList;
import java.util.List;

import org.remus.DataStackRef;
import org.remus.RemusInstance;
import org.remus.manage.WorkStatus;

public class PipeGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(RemusApplet applet,
			RemusInstance instance) {		

		List<String> arrayList = new ArrayList<String>();
		for ( String ref : applet.getInputs() ) {
			String iRef = DataStackRef.pathFromSubmission( applet, ref, instance );
			arrayList.add( iRef );
		}
		applet.datastore.add( applet.getPath() + "/@work", instance.toString(), 0,0, "0", arrayList );


		long t = applet.datastore.getTimeStamp(applet.getPath() + "/@done", instance.toString() );
		WorkStatus.setWorkStat( applet, instance, 0, 0, 0, 1, t);
	}
}
