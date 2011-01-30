package org.remus.applet;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.remus.RemusInstance;
import org.remus.WorkDescription;

public class SplitterApplet extends RemusApplet {

	@Override
	public WorkDescription getWork(RemusInstance inst, int jobID) {
		WorkDescription out = new WorkDescription();
		Map map = new HashMap();
		map.put("input", inputs.get(jobID).getPath() );
		out.desc = map;
		return out;
	}

	@Override
	public Collection<Integer> getWorkSet(RemusInstance remusInstance) {
		Set<Integer> out = new HashSet<Integer>();
		for ( int i = 0; i < inputs.size(); i++ ) {
			out.add(i);
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
