package org.remus.applet;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.WorkDescription;

public class MapGenerator implements WorkGenerator {

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
			if ( applet.hasInputs() ) {
				if ( applet.isReady(instance) ) {
					int jobID = 0;
					for ( InputReference iRef : applet.inputs ) {
						for ( Object key : applet.datastore.listKeys( new File(iRef.getPortPath() ), instance.toString() ) ) {
							Map map = new HashMap();							
							map.put("input", iRef.getPortPath() );
							map.put("key",   key );
							outList.add( new WorkDescription(applet, instance, jobID, map) );
							jobID++;							
						}
					}
				}			
			}
		}		
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
