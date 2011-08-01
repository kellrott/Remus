package org.remus.work;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.remus.RemusInstance;
import org.remus.manage.WorkStatusImpl;
import org.remus.server.DataStackRef;
import org.remusNet.thrift.AppletRef;

public class PipeGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(RemusAppletImpl applet,
			RemusInstance instance) {		
		try {
			AppletRef ar = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() );
			AppletRef arWork = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() + "/@work" );

			List<String> arrayList = new ArrayList<String>();
			for ( String ref : applet.getInputs() ) {
				String iRef = DataStackRef.pathFromSubmission( applet, ref, instance );
				arrayList.add( iRef );
			}
			applet.datastore.add(arWork, 0,0, "0", arrayList );


			long t = applet.datastore.getTimeStamp( ar );
			WorkStatusImpl.setWorkStat( applet, instance, 0, 0, 0, 1, t);
		}catch (TException e){
			e.printStackTrace();
		}
	}
}
