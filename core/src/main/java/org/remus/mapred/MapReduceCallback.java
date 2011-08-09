package org.remus.mapred;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.core.RemusInstance;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.work.Submission;


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
	private RemusDB db;
	private RemusAttach attach;
	private Map jobInfo;
	private String pipeline;

	public MapReduceCallback(String pipeline, Map jobInfo, RemusDB db, RemusAttach attach) {
		emitCount = 0;
		this.jobInfo = jobInfo;
		this.pipeline = pipeline;
		outList = new LinkedList<MapReduceEmit>();
		this.db = db;
		this.attach = attach;
	}

	public void emit(String key, Object val) {
		outList.add(new MapReduceEmit(key, val, emitCount));
		emitCount++;
	}

	public void writeEmits(AppletRef ar, long jobID) throws TException, NotImplemented {
		for (MapReduceEmit mpe : outList) {
			db.add(ar, jobID, mpe.emitID, mpe.key, mpe.val);
		}
	}

	public InputStream open(String key, String name) {
		Map inMap = (Map)jobInfo.get(Submission.InputField);
		try {
			RemusInstance inst = RemusInstance.getInstance(db, pipeline, (String) inMap.get(Submission.InstanceField));
			AppletRef arAttach =  new AppletRef(pipeline, inst.toString(), (String) inMap.get("_applet"));
			return attach.readAttachement(arAttach, key, name);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		// TODO Auto-generated method stub
		return null;
	}
}
