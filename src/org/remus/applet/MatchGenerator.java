package org.remus.applet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.WorkDescription;
import org.remus.WorkReference;

public class MatchGenerator implements WorkGenerator {

	RemusApplet applet;
	List<WorkDescription> outList;
	int curPos;
	@Override
	public void init(RemusApplet applet) {
		this.applet = applet;		
	}

	@Override
	public void startWork(RemusInstance instance, long reqCount) {
		int jobID = 0;
		outList = new ArrayList<WorkDescription>();
		for ( InputReference lRef : applet.lInputs ) {
			String lStr = lRef.getPortPath() + "@data";
			for ( Object key : applet.datastore.listKeys( lStr, instance.toString() ) ) {
				for ( InputReference rRef : applet.rInputs ) {
					Map map = new HashMap();
					map.put("key", key);
					map.put("left_input", lStr );
					map.put("right_input", rRef.getPortPath() + "@reduce" );
					outList.add( new WorkDescription( new WorkReference(applet, instance, jobID), map) );
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
