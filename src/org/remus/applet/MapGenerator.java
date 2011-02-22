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
		if ( !applet.isComplete(instance) ) {
			if ( applet.hasInputs() ) {
				if ( applet.isReady(instance) ) {
					int jobID = 0;
					for ( RemusPath iRef : applet.inputs ) {
						String portPath = null;
						if ( iRef.getInputType() == RemusPath.DynamicInput )
							portPath = applet.getPath() + "@submit";
						else
							portPath =  iRef.getPortPath() +"@data";
						
						long keyCount = applet.datastore.keyCount( portPath, instance.toString() );
						long keysPerJob = keyCount / reqCount;
						if ( keysPerJob == 0)
							keysPerJob = 1;
						long count = 0;
						Map map = null;
						List keyList = null;

						
						for ( Object key : applet.datastore.listKeys( portPath, instance.toString() ) ) {
							if ( count % keysPerJob == 0) {
								map = new HashMap();
								map.put("input", portPath );
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
