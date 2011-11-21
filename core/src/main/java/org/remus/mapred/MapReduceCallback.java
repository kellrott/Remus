package org.remus.mapred;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.core.AppletInput;
import org.remus.core.AppletInstanceRecord;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusInstance;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;


public class MapReduceCallback {

	static class MapReduceEmit {
		String key;
		Object val;
		long emitID;
		String outStack = null;
		public MapReduceEmit(String key, Object val, long emitID, String outStack) {
			this.key = key;
			this.val = val;
			this.emitID = emitID;
			this.outStack = outStack;
		}
	}

	long emitCount;
	List<MapReduceEmit> outList;
	private RemusDB db;
	private RemusAttach attach;
	private AppletInstanceRecord jobInfo;
	private String pipeline;
	private String applet;
	private String instance;

	public MapReduceCallback(String pipeline, String instance, String applet, AppletInstanceRecord jobInfo, RemusDB db, RemusAttach attach) {
		init(pipeline, instance, applet, jobInfo, db, attach);
	}

	public void init(String pipeline, String instance, String applet, AppletInstanceRecord jobInfo, RemusDB db, RemusAttach attach) {
		emitCount = 0;
		this.jobInfo = jobInfo;
		this.pipeline = pipeline;
		this.applet = applet;
		this.instance = instance;
		outList = new LinkedList<MapReduceEmit>();
		this.db = db;
		this.attach = attach;
	}

	public void emit(String key, Object val) {
		outList.add(new MapReduceEmit(key, val, emitCount, null));
		emitCount++;
	}

	public void emit(String key, Object val, String outStack) {
		outList.add(new MapReduceEmit(key, val, emitCount, outStack));
		emitCount++;
	}


	public void writeEmits(AppletRef ar, long jobID) throws TException, NotImplemented {
		for (MapReduceEmit mpe : outList) {
			if (mpe.outStack == null) {
				db.add(ar, jobID, mpe.emitID, mpe.key, mpe.val);
			} else {
				//BUG: no validation done to make sure that outstack was actually decalared in the 
				//pipeline description 
				AppletRef or = new AppletRef(pipeline, instance, applet + ":" + mpe.outStack);
				db.add(or, jobID, mpe.emitID, mpe.key, mpe.val);
			}
		}
		outList.clear();
	}

	/*
	public InputStream openInput(String key, String name) {
		Map inMap = (Map)jobInfo.get(PipelineSubmission.InputField);
		try {
			RemusInstance inst = RemusInstance.getInstance(db, pipeline, (String) inMap.get(PipelineSubmission.INSTANCE_FIELD));
			AppletRef arAttach =  new AppletRef(pipeline, inst.toString(), (String) inMap.get("_applet"));
			return attach.readAttachment(arAttach, key, name);
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
	 */

	public void copyTo(File file, String key, String name) {
		RemusInstance inst;
		try {			
			inst = RemusInstance.getInstance(db, pipeline, jobInfo.getInstance());
			AppletRef arAttach =  new AppletRef(pipeline, inst.toString(), applet);
			attach.copyTo(file, arAttach, key, name);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void copyFrom(File path, String key, String name) {
		try {
			AppletInput input = jobInfo.getInput(jobInfo.getSource(), db);
			AppletRef arAttach =  new AppletRef(input.getPipeline(), input.getInstance().toString(), input.getApplet());
			attach.copyFrom(path, arAttach, key, name);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public RemusDB getDatabase() {
		return db;
	}

	public String getPipeline() {
		return pipeline;
	}

	public String getApplet() {
		return applet;
	}

	public AppletInstanceRecord getJobInfo() {
		return jobInfo;
	}

	public RemusAttach getAttachStore() {
		return attach;		
	}

	public String getInstance() {
		return instance;
	}
}
