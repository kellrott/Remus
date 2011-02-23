package org.remus.applet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.remus.RemusPath;
import org.remus.RemusInstance;
import org.remus.WorkDescription;
import org.remus.WorkReference;

public class MapGenerator implements WorkGenerator {

	ArrayList<WorkDescription> outList;
	int curPos;
	RemusApplet applet;
	@Override
	public void init(RemusApplet applet) {
		this.applet = applet;
	}

	@Override
	public void startWork(RemusInstance instance, long reqCount) {
		outList = new ArrayList<WorkDescription>();	
		int jobID = 0;
		for ( RemusPath ref : applet.inputs ) {
			RemusPath iRef = new RemusPath( ref, instance );						
			long keyCount = iRef.getKeyCount( applet.datastore );
			long keysPerJob = keyCount / reqCount;
			if ( keysPerJob == 0)
				keysPerJob = 1;
			long count = 0;
			Map map = null;
			List keyList = null;						
			for ( Object key : iRef.listKeys( applet.datastore ) ) {
				if ( count % keysPerJob == 0) {
					map = new HashMap();
					map.put("input", iRef.getInstancePath() );
					keyList = new ArrayList();
					map.put("key", keyList );
					outList.add( new WorkDescription( new WorkReference(applet, instance, jobID), map) );
				}
				count++;
				keyList.add( key );
				jobID++;							
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
