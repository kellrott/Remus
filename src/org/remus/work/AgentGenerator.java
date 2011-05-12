package org.remus.work;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.remus.RemusInstance;
import org.remus.RemusPath;

public class AgentGenerator  implements WorkGenerator {
	RemusApplet applet;
	RemusInstance inst;
	boolean done;
	@Override
	public Set<WorkKey> getActiveKeys(RemusApplet applet, RemusInstance instance, long reqCount) {
		done = false;
		this.applet = applet;
		this.inst = instance;
		Set<WorkKey> outList = new HashSet<WorkKey>();
		int jobID = 0;
		int errorCount = 0;
		int doneCount = 0;
		System.out.println("AGENT WORK");
		for ( RemusPath input : applet.getInputs() ) {
			//RemusPath iRef = new RemusPath( input, instance );			
			WorkKey w = new WorkKey(instance, jobID);
			w.jobID = jobID;
			w.key = instance.toString() + "/" + input.getApplet();			
			outList.add( w );	
			jobID++;
		}
		
		long t = applet.datastore.getTimeStamp(applet.getPath(), instance.toString() );
		AppletInstanceStatusView stat = new AppletInstanceStatusView(applet);
		stat.setWorkStat( instance, doneCount, errorCount, jobID, t);
		
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
				for ( WorkKey key : keys ) {
					out.put(key.jobID, key.key);
				}
				return out;
			}
		};
	}

}
