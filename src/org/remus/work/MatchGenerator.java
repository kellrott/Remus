package org.remus.work;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.remus.RemusPath;
import org.remus.RemusInstance;

public class MatchGenerator implements WorkGenerator {

	RemusApplet applet;
	RemusInstance inst;
	boolean done;

	@Override
	public Set<WorkKey> getActiveKeys(RemusApplet applet, RemusInstance instance, long reqCount) {
		done = false;
		this.applet = applet;
		this.inst = instance;

		Set<WorkKey> outList = new HashSet<WorkKey>();
		int jobID = 0;
		int errorCount = 0;
		int doneCount = 0;

		RemusPath lRef = new RemusPath( applet.getLeftInput(), instance );
		RemusPath rRef = new RemusPath( applet.getRightInput(), instance );
		for ( String key : lRef.listKeys( applet.datastore ) ) {
			if ( outList.size() < reqCount ) {		
				if ( !applet.datastore.containsKey( applet.getPath() + "@done", instance.toString(), Integer.toString(jobID)) ) {
					if (!applet.datastore.containsKey( applet.getPath() + "@error", instance.toString(), Integer.toString(jobID)) ) {
						WorkKey w = new WorkKey( instance, jobID ) ;
						w.key = key;
						w.lPathStr = lRef.getPath();
						w.rPathStr = rRef.getPath();
						outList.add( w );
						jobID++;
					} else {
						errorCount++;
					}
				} else {
					doneCount++;
				}
			}
		}
		Map stat = new HashMap();
		stat.put("done", doneCount);
		stat.put("error", errorCount);
		stat.put("total", jobID);
		applet.datastore.add( applet.getPath() + "@status", RemusInstance.STATIC_INSTANCE_STR, 0L, 0L, instance.toString(), stat );
		if ( outList.size() == 0 && reqCount > 0 )
			done = true;
		return outList;
	}
	
	@Override
	public boolean isDone() {
		return done;
	}


	@Override
	public AppletInstance getAppletInstance() {
		return new AppletInstance(applet, inst) {
			@Override
			public Object formatWork(Set<WorkKey> keys) {
				Map map = new HashMap();
				Map keyMap = new HashMap();
				String lPathStr = null;
				String rPathStr = null;
				for ( WorkKey key : keys ) {
					lPathStr = key.lPathStr;
					rPathStr = key.rPathStr;
					keyMap.put(key.jobID, key.key);
				}
				map.put("key", keyMap);
				map.put("left_input", lPathStr );
				map.put("right_input", rPathStr );
				return map;
			}
		};
	}



}
