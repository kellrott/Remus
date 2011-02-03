package org.remus.applet;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.WorkDescription;

public class ReduceGenerator implements WorkGenerator {

	List<WorkDescription> outList;
	int curPos;
	@Override
	public void startWork(RemusInstance instance) {
		int jobID = 0;
		outList = new ArrayList<WorkDescription>();
		for ( InputReference iRef : applet.getInputs() ) {
			for ( Object key : applet.datastore.listKeys( new File(iRef.getPortPath()), instance.toString() ) ) {
				Map map = new HashMap();
				map.put("input", iRef.getPortPath() );
				map.put("key", key );
				outList.add( new WorkDescription(applet, instance, jobID, map) );
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
