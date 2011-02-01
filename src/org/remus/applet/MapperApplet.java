package org.remus.applet;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.mpstore.KeyValuePair;
import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.WorkDescription;

public class MapperApplet extends RemusApplet {

	@Override
	public WorkDescription getWork(RemusInstance inst, int jobID) {
		WorkDescription out = new WorkDescription();
		Map map = new HashMap();
		
		long keyCount = 0;
		for ( InputReference iRef : inputs ) {
			RemusApplet iApplet = getPipeline().getApplet(iRef.getPath());
			for ( KeyValuePair pair : datastore.listKeyPairs( new File(iApplet.getPath()), inst.toString() ) ) {
				if ( keyCount == jobID ) {
					//TODO:Find less stupid way to do this
					map.put("input", iRef.getPath() );
					map.put("jobID", pair.getJobID() );
					map.put("emitID", pair.getEmitID() );
				}
				keyCount++;
			}						
		}		

		out.desc = map;
		return out;
	}

	@Override
	public Collection<Long> getWorkSet(RemusInstance remusInstance) {
		Set<Long> out = new HashSet<Long>();
		if ( !isComplete(remusInstance) ) {
			if ( hasInputs() ) {
				if ( isReady(remusInstance) ) {
					long keyCount = 0;
					for ( InputReference iRef : inputs ) {
						RemusApplet iApplet = getPipeline().getApplet(iRef.getPath());
						for ( Object key : datastore.listKeys( new File(iApplet.getPath()), remusInstance.toString() ) ) {
							out.add(keyCount);
							keyCount++;
						}						
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
		return out;
	}


	@Override
	public Map getDescMap() {
		Map out = new HashMap();
		out.put("mode", "map");
		return out;
	}


}
