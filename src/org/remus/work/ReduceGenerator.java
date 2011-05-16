package org.remus.work;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.remus.DataStackRef;
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
		DataStackRef iRef = DataStackRef.fromSubmission(applet, applet.getInput(), instance);
		Set<WorkKey> keyList = new HashSet<WorkKey>();
		int jobID = 0;
		int doneCount = 0;
		int errorCount = 0;
		for ( String key : iRef.listKeys( applet.datastore  ) ) {
			if ( keyList.size() < reqCount ) {
				if ( !applet.datastore.containsKey( applet.getPath() + "/@done", instance.toString(), Integer.toString(jobID)) ) {
					if ( !applet.datastore.containsKey( applet.getPath() + "/@error", instance.toString(), Integer.toString(jobID)) ) {
						WorkKey w =  new WorkKey( instance, jobID );
						w.key = key;
						//w.pathStr = iRef.getPath();
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

		long t = applet.datastore.getTimeStamp(applet.getPath(), instance.toString() );
		AppletInstanceStatusView stat = new AppletInstanceStatusView(applet);
		stat.setWorkStat( instance, doneCount, errorCount, jobID, t);
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
				String pathStr = null;
				for ( WorkKey key : keys ) {
					out.put(key.jobID, key.key);
				}
				return out;
			}
		};		
	}
}
