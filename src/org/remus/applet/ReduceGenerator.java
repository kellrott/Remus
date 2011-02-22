package org.remus.applet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.WorkDescription;
import org.remus.WorkReference;

public class ReduceGenerator implements WorkGenerator {

	List<WorkDescription> outList;
	int curPos;
	@Override
	public void startWork(RemusInstance instance, long reqCount) {
		int jobID = 0;
		outList = new ArrayList<WorkDescription>();		
		for ( InputReference iRef : applet.getInputs() ) {
			long keyCount = applet.datastore.keyCount( iRef.getPortPath() + "@data", instance.toString() );
			long keysPerJob = keyCount / reqCount;
			if ( keysPerJob == 0 )
				keysPerJob = 1;
			long count = 0;
			Map map = null;
			List keyList = null;
			for ( Object key : applet.datastore.listKeys( iRef.getPortPath() + "@data", instance.toString() ) ) {
				if ( count % keysPerJob == 0) {
					map = new HashMap();
					map.put("input", iRef.getPortPath() + "@data" );
					keyList = new ArrayList();
					map.put("key", keyList );
					outList.add( new WorkDescription( new WorkReference(applet, instance, jobID), map) );
				}
				count++;
				keyList.add(key);
				jobID++;
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

	RemusApplet applet;
	@Override
	public void init(RemusApplet applet) {
		this.applet = applet;		
	}


}
