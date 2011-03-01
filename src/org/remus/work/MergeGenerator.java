package org.remus.work;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.remus.RemusPath;
import org.remus.RemusInstance;

public class MergeGenerator implements WorkGenerator {
	RemusApplet applet;
	RemusInstance inst;
	boolean done;
	@Override
	public boolean isDone() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<WorkKey> getActiveKeys(RemusApplet applet,
			RemusInstance instance, long reqCount) {
		int jobID = 0;
		Set<WorkKey> outList = new HashSet<WorkKey>();
		done = true;
		this.applet = applet;
		this.inst = instance;
		RemusPath lRef = new RemusPath( applet.getLeftInput(), instance );		
		for ( String key : lRef.listKeys(applet.datastore) ) {
			if ( outList.size() < reqCount ) {
				WorkKey w = new WorkKey( instance, jobID);
				w.key = key;
				outList.add( w );
			} else {
				done = false;
			}
			jobID++;
		}
		return outList;	
	}

	@Override
	public AppletInstance getAppletInstance() {
		return new AppletInstance(applet, inst) {
			@Override
			public Object formatWork(Set<WorkKey> keys) {
				return new HashMap();
			}
		};
	}
}
