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

	@Override
	public Set<WorkKey> getActiveKeys(RemusApplet applet, RemusInstance instance, long reqCount) {
		Set<WorkKey> outList;
		int jobID = 0;
		outList = new HashSet<WorkKey>();
		for ( RemusPath lRef : applet.lInputs ) {
			String lStr = lRef.getPortPath() + "@data";
			for ( String key : applet.datastore.listKeys( lStr, instance.toString() ) ) {		
				WorkKey w = new WorkKey( instance, jobID ) ;
				w.key = key;
				outList.add( w );
				jobID++;
			}

		}
		return outList;
	}

	@Override
	public boolean isDone() {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public AppletInstance getAppletInstance() {
return new AppletInstance(applet, inst) {
	
	@Override
	public Object formatWork(Set<WorkKey> keys) {
		Map map = new HashMap();
		map.put("key", keys);
		map.put("left_input", applet.lInputs.get(0).getViewPath() );
		map.put("right_input", applet.rInputs.get(0) + "@reduce" );
		return map;
	}
};
	}

	

}
