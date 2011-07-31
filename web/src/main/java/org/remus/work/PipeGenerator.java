package org.remus.work;

import java.util.ArrayList;
import java.util.List;

import org.remus.RemusInstance;
import org.remus.manage.WorkStatusImpl;
import org.remus.server.DataStackRef;

public class PipeGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(RemusAppletImpl applet,
			RemusInstance instance) {		

		List<String> arrayList = new ArrayList<String>();
		for ( String ref : applet.getInputs() ) {
			String iRef = DataStackRef.pathFromSubmission( applet, ref, instance );
			arrayList.add( iRef );
		}
		applet.datastore.add( applet.getPath() + "/@work", instance.toString(), 0,0, "0", arrayList );


		long t = applet.datastore.getTimeStamp(applet.getPath() + "/@done", instance.toString() );
		WorkStatusImpl.setWorkStat( applet, instance, 0, 0, 0, 1, t);
	}
}
