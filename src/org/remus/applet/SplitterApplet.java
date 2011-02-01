package org.remus.applet;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.remus.RemusInstance;
import org.remus.WorkDescription;

public class SplitterApplet implements WorkGenerator {


	@Override
	public Map getDescMap() {
		Map out = new HashMap();
		out.put("mode", "split");
		return out;
	}

	ArrayList<WorkDescription> outList;
	int curPos;
	RemusApplet applet;
	@Override
	public void init(RemusApplet applet) {
		this.applet = applet;	
	}

	
	@Override
	public void startWork(RemusInstance instance) {
		outList = new ArrayList<WorkDescription>();
		if ( !applet.isComplete(instance) ) {
			if ( applet.isReady(instance) ) {
				if ( applet.hasInputs() ) {
					for ( int i = 0; i < applet.inputs.size(); i++ ) {
						KeyValuePair d = applet.datastore.get(new File("/@work"), instance.toString(),i, 0 );
						if ( d == null ) {
							Map out = new HashMap();
							out.put( "input", applet.inputs.get(i).getPath() );
							outList.add( new WorkDescription(applet, instance, i, out) );
						}
					}
				} else {
					//out.add(0L);
				}	
			}
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
