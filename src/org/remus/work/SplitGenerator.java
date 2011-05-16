package org.remus.work;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.remus.DataStackRef;
import org.remus.RemusPath;
import org.remus.RemusInstance;

public class SplitGenerator implements WorkGenerator {
	RemusApplet applet;
	RemusInstance inst;
	boolean done;
	@Override
	public Set<WorkKey> getActiveKeys(RemusApplet applet,
			RemusInstance instance, long reqCount) {
		this.applet = applet;	
		this.inst = instance;
		done = false;
		Set<WorkKey> outList = new HashSet<WorkKey>();
		if ( applet.datastore.containsKey(applet.getPath() + "/@error", instance.toString(), "0" ) ) {
			return outList;			
		}
		if ( applet.datastore.containsKey(applet.getPath() + "/@done", instance.toString(), "0" ) ) {
			done = true;
		}
		if ( !done ) {
			outList.add( new WorkKey( instance, 0) );				
		}
		long t = applet.datastore.getTimeStamp(applet.getPath() + "/@done", instance.toString() );
		AppletInstanceStatusView stat = new AppletInstanceStatusView(applet);
		stat.setWorkStat( instance, 0, 0, 1, t);
		return outList;
	}


	@Override
	public AppletInstance getAppletInstance() {
		return new AppletInstance(applet,inst) {		
			@Override
			public Object formatWork(Set<WorkKey> keys) {				
				Map obj = new HashMap();
				for ( WorkKey wk : keys ) {
					obj.put(wk.jobID, wk.pathStr);
				}
				return obj;				
			}
		};
	}

	@Override
	public boolean isDone() {
		return done;
	}

}
