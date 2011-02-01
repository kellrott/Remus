package org.remus.applet;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.WorkDescription;

public class ReducerApplet implements WorkGenerator {

	/*
	@Override
	public WorkDescription getWork(RemusInstance remusInstance, int jobID) {
		Map<String,List<Object>> out = new HashMap<String,List<Object>>();
		if ( !isComplete(remusInstance) ) {
			if ( hasInputs() ) {
				if ( isReady(remusInstance) ) {
					long keyCount = 0;
					for ( InputReference iRef : inputs ) {
						RemusApplet iApplet = getPipeline().getApplet(iRef.getPath());
						List<Object> a = new ArrayList<Object>();
						for ( Object key : datastore.listKeys( new File(iApplet.getPath()), remusInstance.toString() ) ) {
							if ( keyCount == jobID ) {
								a.add(key);
							}
							keyCount++;
						}				
						out.put(iRef.getPath(), a);
					}
				}			
			}			
			Iterable<Object> completeJobs = datastore.get(new File("/@work"), remusInstance.toString(), getPath() );
			for ( Object job : completeJobs ) {
				out.remove(((Long)job).intValue());
			}
			if ( out.size() == 0 ) {
				setDone( remusInstance );				
			}			
		}
		WorkDescription wd = new WorkDescription();
		wd.desc = out;
		return wd;
	}


	@Override
	public Map getDescMap() {
		Map out = new HashMap();
		out.put("mode", "reduce");
		return out;
	}

	
	RemusApplet applet;
	RemusInstance instance;
	
	@Override
	public void init(RemusApplet applet, RemusInstance instance) {
		this.applet = applet;
		this.instance = instance;
		if ( !applet.isComplete(instance) ) {
			if ( applet.hasInputs() ) {
				if ( applet.isReady(instance) ) {
					long keyCount = 0;
					for ( InputReference iRef : applet.getInputs() ) {
						RemusApplet iApplet = applet.getPipeline().getApplet(iRef.getPath());
						List<Long> a = new ArrayList<Long>();
							for ( Object key : applet.datastore.listKeys( new File(iApplet.getPath()), instance.toString() ) ) {
							a.add(keyCount);
							keyCount++;
						}
						out.put(iRef.getPath(), a);
					}
					
				}			
			}
			Iterable<Object> completeJobs = datastore.get(new File("/@work"), remusInstance.toString(), getPath() );
			for ( Object job : completeJobs ) {
				out.remove(((Long)job).intValue());
			}
			if ( out.size() == 0 ) {
				setDone( remusInstance );				
			}			
		}		
		
	}
*/
	
	
	@Override
	public void startWork(RemusInstance instance) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public WorkDescription nextWork() {		
		return null;
	}

	@Override
	public Map getDescMap() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init(RemusApplet applet) {
		// TODO Auto-generated method stub
		
	}


}
