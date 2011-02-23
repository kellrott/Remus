package org.remus.applet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.remus.RemusPath;
import org.remus.RemusInstance;
import org.remus.WorkDescription;
import org.remus.WorkReference;

public class SplitGenerator implements WorkGenerator {

	ArrayList<WorkDescription> outList;
	int curPos;
	RemusApplet applet;
	@Override
	public void init(RemusApplet applet) {
		this.applet = applet;	
	}


	@Override
	public void startWork(RemusInstance instance, long reqQuest) {
		outList = new ArrayList<WorkDescription>();
		if ( applet.hasInputs() ) {
			for ( int i = 0; i < applet.inputs.size(); i++ ) {
				//KeyValuePair d = applet.datastore.get(new File("/@work"), instance.toString(),i, 0 );
				//if ( d == null ) {
				Map out = new HashMap();
				if ( applet.inputs.get(i).getInputType() == RemusPath.AppletInput )
					out.put( "input", applet.inputs.get(i).getInstancePath() );
				if ( applet.inputs.get(i).getInputType() == RemusPath.ExternalInput )
					out.put( "input", applet.inputs.get(i).getURL() );

				outList.add( new WorkDescription( new WorkReference(applet, instance, i), out) );
				//}
			}
		} else {
			//KeyValuePair d = applet.datastore.get(new File("/@work"), instance.toString(),0, 0 );
			//if ( d == null ) {
			Map out = new HashMap();
			out.put( "input", null );
			outList.add( new WorkDescription( new WorkReference(applet, instance, 0), out) );	
			//}	
		}
		curPos = 0;
	}

	@Override
	public WorkDescription nextWork() {
		if ( curPos < outList.size() ) {
			curPos++;
			return outList.get(curPos-1);
		}
		return null;
	}

}
