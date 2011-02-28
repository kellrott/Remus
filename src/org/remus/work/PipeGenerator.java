package org.remus.work;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.remus.RemusInstance;
import org.remus.RemusPath;

public class PipeGenerator implements WorkGenerator {
	int curPos;
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
		Set<WorkKey> outList = new HashSet<WorkKey>();
		if ( applet.datastore.containsKey(applet.getPath() + "@done", instance.toString(), "0" ) ) {
			done = true;
			return outList;
		}		
		List<String> arrayList = new ArrayList();
		for ( RemusPath iRef : applet.getInputs() ) {
			arrayList.add( iRef.getURL() );
			}
			WorkKey w =  new WorkKey(instance, 0);
			outList.add( w );
			w.pathArray = arrayList;
			//}
			//}
		return outList;		
	}

	@Override
	public AppletInstance getAppletInstance() {
		return new AppletInstance(applet, inst) {	
			@Override
			public Object formatWork(Set<WorkKey> keys) {
				Map out = new HashMap();
				List outList = null;
				for (WorkKey wk : keys) {
					outList = wk.pathArray;
				}
				out.put( "input", outList );	
				return out;
			}
		};		
	}		





}
