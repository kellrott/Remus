package org.remus.work;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.remus.RemusPath;
import org.remus.RemusInstance;

public class ReduceGenerator implements WorkGenerator {
	RemusApplet applet;
	RemusInstance inst;
	boolean done;
	@Override
	public boolean isDone() {
		return done;
	}

	@Override
	public Set<WorkKey> getActiveKeys(RemusApplet applet,
			RemusInstance instance, long reqCount) {
		this.applet = applet;
		this.inst = instance;
		done = false;
		RemusPath iRef = new RemusPath(applet.getInput(), instance);
		Set<WorkKey> keyList = new HashSet<WorkKey>();
		int jobID = 0;
		int doneCount = 0;
		int errorCount = 0;
		for ( String key : iRef.listKeys( applet.datastore  ) ) {
			if ( keyList.size() < reqCount ) {
				if ( !applet.datastore.containsKey( applet.getPath() + "@done", instance.toString(), Integer.toString(jobID)) ) {
					if ( !applet.datastore.containsKey( applet.getPath() + "@error", instance.toString(), Integer.toString(jobID)) ) {
						WorkKey w =  new WorkKey( instance, jobID );
						w.key = key;
						w.pathStr = iRef.getPath();
						keyList.add( w );				
					} else {
						errorCount += 1;
					}
				} else {
					doneCount += 1;
				}
			}
			jobID++;
		}
		Map stat = new HashMap();
		stat.put("done", doneCount);
		stat.put("error", errorCount);
		stat.put("total", jobID);
		applet.datastore.add( applet.getPath() + "@status", RemusInstance.STATIC_INSTANCE_STR, 0L, 0L, instance.toString(), stat );
		if ( keyList.size() == 0 && reqCount > 0 )
			done = true;
		return keyList;
	}

	@Override
	public AppletInstance getAppletInstance() {
		return new AppletInstance(applet, inst) {		
			@Override
			public Object formatWork(Set<WorkKey> keys) {
				Map out = new HashMap();		
				Map keyMap = new HashMap();
				String pathStr = null;
				for ( WorkKey key : keys ) {
					pathStr = key.pathStr;
					keyMap.put(key.jobID, key.key);
				}
				out.put( "input" , pathStr );
				out.put( "key", keyMap );
				return out;
			}
		};		
	}
}
