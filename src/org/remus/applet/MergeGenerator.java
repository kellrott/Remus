package org.remus.applet;


import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.WorkDescription;

public class MergeGenerator implements WorkGenerator {
	RemusApplet applet;
	List<WorkDescription> outList;
	int curPos;
	@Override
	public void init(RemusApplet applet) {
		this.applet = applet;		
	}

	@Override
	public void startWork(RemusInstance instance) {
		int jobID = 0;
		outList = new ArrayList<WorkDescription>();
		for ( InputReference lRef : applet.lInputs ) {
			for ( Object key : applet.datastore.listKeys( new File(lRef.getPortPath()), instance.toString() ) ) {
				for ( InputReference rRef : applet.rInputs ) {
					Map map = new HashMap();
					map.put("left_key", key);
					map.put("left_input", lRef.getPortPath() );
					map.put("right_input", rRef.getPortPath() );
					outList.add( new WorkDescription(applet, instance, jobID, map) );
					jobID++;
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
