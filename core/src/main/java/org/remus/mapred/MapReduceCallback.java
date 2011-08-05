package org.remus.mapred;

import java.util.LinkedList;
import java.util.List;

import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;


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
	
	public void writeEmits(RemusDB datastore, AppletRef ar, long jobID) throws TException, NotImplemented {
		for (MapReduceEmit mpe : outList) {
			datastore.add(ar, jobID, mpe.emitID, mpe.key, mpe.val);
		}
	}
}
