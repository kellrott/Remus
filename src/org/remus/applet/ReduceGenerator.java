package org.remus.applet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.remus.RemusPath;
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
		for ( RemusPath ref : applet.getInputs() ) {
			RemusPath iRef = new RemusPath(ref, instance);
			long keyCount = iRef.getKeyCount( applet.datastore, (int)reqCount * 1000 );
			long keysPerJob = keyCount / reqCount;
			if ( keysPerJob == 0 )
				keysPerJob = 1;
			long count = 0;
			Map map = null;
			List keyList = null;
			for ( Object key : iRef.listKeys( applet.datastore  ) ) {
				if ( count % keysPerJob == 0) {
					map = new HashMap();
					map.put("input", iRef.getInstancePath() );
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
