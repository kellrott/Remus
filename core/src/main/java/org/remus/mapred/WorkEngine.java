package org.remus.mapred;

import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusDB;
import org.remus.thrift.AppletRef;
import org.remus.thrift.JobStatus;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.WorkMode;

public class WorkEngine {
	WorkDesc work;
	RemusDB db;
	MapReduceFunction mapred;
	JobStatus status;
	public WorkEngine(WorkDesc work, RemusDB db, MapReduceFunction mapred) {
		this.work = work;
		this.db = db;
		this.mapred = mapred;
		status = JobStatus.QUEUED;
	}

	public void start() {
		AppletRef arWork = new AppletRef(work.workStack.pipeline, 
				work.workStack.instance, work.workStack.applet + "/@work");
		status = JobStatus.WORKING;

		Map stackInfo = (Map) JSON.loads(work.infoJSON);
		mapred.init(stackInfo);
		try { 
			if (work.mode == WorkMode.MAP) {
				Map inputInfo = (Map) stackInfo.get("_input");
				AppletRef ar = new AppletRef(
						work.workStack.pipeline,
						(String) inputInfo.get("_instance"),
						(String) inputInfo.get("_applet")
						);
				AppletRef outRef = new AppletRef(
						work.workStack.pipeline,
						work.workStack.instance,
						work.workStack.applet
				);
				for (long jobID : work.jobs) {
					for (Object key : db.get(arWork, Long.toString(jobID))) {
						MapReduceCallback cb = new MapReduceCallback();
						for (Object value : db.get(ar, (String) key)) {
							mapred.map((String) key, value, cb);
						}
						cb.writeEmits(db, outRef, jobID);
					}
				}
			}
		} catch (TException e) {
			status = JobStatus.ERROR;
		} catch (NotImplemented e) {
			status = JobStatus.ERROR;
		} catch (NotSupported e) {
			status = JobStatus.ERROR;
		}
	}

}
