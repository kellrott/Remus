package org.remus.work;


import java.util.HashSet;
import java.util.Set;

import org.remus.RemusPath;
import org.remus.RemusInstance;

public class MergeGenerator implements WorkGenerator {
	RemusApplet applet;
	RemusInstance inst;

	@Override
	public boolean isDone() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<WorkKey> getActiveKeys(RemusApplet applet,
			RemusInstance instance, long reqCount) {
		int jobID = 0;
		Set<WorkKey> outList;
		outList = new HashSet<WorkKey>();
		RemusPath lRef = new RemusPath( applet.getLeftInput(), instance );		
		for ( String key : lRef.listKeys(applet.datastore) ) {
			WorkKey w = new WorkKey( instance, jobID);
			w.key = key;
			outList.add( w );
		}
		jobID++;
		return outList;	
	}

	@Override
	public AppletInstance getAppletInstance() {
		// TODO Auto-generated method stub
		return null;
	}
}
