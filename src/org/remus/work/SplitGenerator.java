package org.remus.work;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
			if ( applet.hasInputs() ) {
				RemusPath iRef = applet.getInput();		
				String pathStr = null;
				if ( iRef.getInputType() == RemusPath.AppletInput )
					pathStr = iRef.getInstancePath();
				if ( iRef.getInputType() == RemusPath.AttachInput )
					pathStr = iRef.getPath();

				WorkKey w =  new WorkKey(instance, 0);
				outList.add( w );
				w.pathStr = pathStr;
				//}
				//}
			} else {
				outList.add( new WorkKey( instance, 0) );	
			}
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
				/*
				Map obj = new HashMap();
				Map pathMap = new HashMap();
				for ( WorkKey wk : keys ) {
					pathMap.put(wk.jobID, wk.pathStr);
				}
				obj.put("input", pathMap );
				return obj;
				 */
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
