package org.remus.applet;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.WorkDescription;
import org.remus.applet.InstanceStatus.NodeInstanceStatus;

public class SplitterApplet extends RemusApplet {

	@Override
	public WorkDescription getWork(RemusInstance inst, int jobID) {
		WorkDescription out = new WorkDescription();
		Map map = new HashMap();
		if ( hasInputs() )
			map.put("input", inputs.get(jobID).getPath() );
		else
			map.put("input", null );

		out.desc = map;
		return out;
	}

	@Override
	public Collection<Long> getWorkSet(RemusInstance remusInstance) {
		Set<Long> out = new HashSet<Long>();
		if ( !isComplete(remusInstance) ) {
			if ( isReady(remusInstance) ) {
				if ( hasInputs() ) {
					for ( long i = 0; i < inputs.size(); i++ ) {
						out.add(i);
					}
				} else {
					out.add(0L);
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
		out.put("mode", "split");
		return out;
	}

}
