package org.remus.work;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.remus.DataStackRef;
import org.remus.RemusInstance;

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
		if ( applet.datastore.containsKey(applet.getPath() + "/@error", instance.toString(), "0" ) ) {
			return outList;			
		}
		if ( applet.datastore.containsKey(applet.getPath() + "/@done", instance.toString(), "0" ) ) {
			done = true;
		}		
		if ( !done ) {
			List<String> arrayList = new ArrayList();
			for ( String ref : applet.getInputs() ) {
				String iRef = DataStackRef.pathFromSubmission( applet, ref, instance );
				arrayList.add( iRef );
			}
			WorkKey w =  new WorkKey(instance, 0);
			outList.add( w );
			w.pathArray = arrayList;
		}

		long t = applet.datastore.getTimeStamp(applet.getPath() + "/@done", instance.toString() );
		AppletInstanceStatusView stat = new AppletInstanceStatusView(applet);
		stat.setWorkStat( instance, 0, 0, 1, t);

		return outList;		
	}

	@Override
	public AppletInstance getAppletInstance() {
		return new AppletInstance(applet, inst) {	
			@Override
			public Object formatWork(Set<WorkKey> keys) {
				Map out = new HashMap();
				for (WorkKey wk : keys) {
					out.put( "0", wk.pathArray );
				}
				return out;
			}
		};		
	}		





}
