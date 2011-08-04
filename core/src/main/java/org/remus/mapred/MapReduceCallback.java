package org.remus.mapred;

import java.util.LinkedList;
import java.util.List;


public class MapReduceCallback {

	static class MapReduceEmit {
		String key;
		Object val;
		long emitID;
		public MapReduceEmit(String key, Object val, long emitID) {
			this.key = key;
			this.val = val;
			this.emitID = emitID;
		}
	}
	
	long emitCount;
	List<MapReduceEmit> outList;
	public MapReduceCallback() {
		emitCount = 0;
		outList = new LinkedList<MapReduceEmit>();
	}
	
	public void emit(String key, Object val) {
		outList.add(new MapReduceEmit(key, val, emitCount));
		emitCount++;
	}
}
