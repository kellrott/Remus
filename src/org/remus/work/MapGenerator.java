package org.remus.work;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.remus.RemusPath;
import org.remus.RemusInstance;

public class MapGenerator implements WorkGenerator {
	RemusApplet applet;
	RemusInstance inst;
	boolean done;
	@Override
	public Set<WorkKey> getActiveKeys(RemusApplet applet, RemusInstance instance, long reqCount) {
		done = false;
		this.applet = applet;
		this.inst = instance;
		Set<WorkKey> outList = new HashSet<WorkKey>();
		RemusPath iRef = new RemusPath( applet.getInput(), instance );
		int jobID = 0;
		int errorCount = 0;
		int doneCount = 0;
		for ( String key : iRef.listKeys( applet.datastore ) ) {
			if ( outList.size() < reqCount ) {
				if ( !applet.datastore.containsKey( applet.getPath() + "/@done", instance.toString(), Integer.toString(jobID)) ) {
					if (!applet.datastore.containsKey( applet.getPath() + "/@error", instance.toString(), Integer.toString(jobID)) ) {
						WorkKey w = new WorkKey(instance, jobID);
						w.key = key;
						w.pathStr = iRef.getPath();
						outList.add( w );					
					} else {
						errorCount++;
					}
				} else {
					doneCount++;
				}
			}
			jobID++;							
		}
		
		InstanceStatusView stat = new InstanceStatusView(applet);
		stat.setWorkStat( instance, doneCount, errorCount, jobID);
		
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
