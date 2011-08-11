package org.remus.mapred;

import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.core.RemusInstance;
import org.remus.thrift.AppletRef;
import org.remus.thrift.JobStatus;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.WorkMode;

public class WorkEngine implements Runnable {
	private WorkDesc work;
	private RemusDB db;
	private MapReduceFunction mapred;
	private JobStatus status;
	private RemusAttach attach;
	public WorkEngine(WorkDesc work, RemusDB db, RemusAttach attach, MapReduceFunction mapred) {
		this.work = work;
		this.db = db;
		this.attach = attach;
		this.mapred = mapred;
		status = JobStatus.QUEUED;
	}

	@Override
	public void run() {
		AppletRef arWork = new AppletRef(work.workStack.pipeline, 
				work.workStack.instance, work.workStack.applet + "/@work");
		status = JobStatus.WORKING;

		Map stackInfo = (Map) JSON.loads(work.infoJSON);
		mapred.init(stackInfo);
		try { 
			if (work.mode == WorkMode.MAP) {
				Map inputInfo = (Map) stackInfo.get("_input");
				RemusInstance inst = RemusInstance.getInstance(db, 
						work.workStack.pipeline, 
						(String) inputInfo.get("_instance"));
				AppletRef ar = new AppletRef(
						work.workStack.pipeline,
						inst.toString(),
						(String) inputInfo.get("_applet")
						);
				AppletRef outRef = new AppletRef(
						work.workStack.pipeline,
						work.workStack.instance,
						work.workStack.applet
				);
				for (long jobID : work.jobs) {
					for (Object key : db.get(arWork, Long.toString(jobID))) {
						MapReduceCallback cb = new MapReduceCallback(work.workStack.pipeline, work.workStack.applet, stackInfo, db, attach);
						for (Object value : db.get(ar, (String) key)) {
							mapred.map((String) key, value, cb);
						}
						cb.writeEmits(outRef, jobID);
					}
				}
				status = JobStatus.DONE;
			} else if (work.mode == WorkMode.SPLIT) {
				MapReduceCallback cb = new MapReduceCallback(work.workStack.pipeline, work.workStack.applet, stackInfo, db, attach);
				mapred.split(stackInfo, cb);
				status = JobStatus.DONE;
			} else if (work.mode == WorkMode.REDUCE) {
				status = JobStatus.DONE;
			} else if (work.mode == WorkMode.MATCH) {
				status = JobStatus.DONE;
			} else if (work.mode == WorkMode.MERGE) {
				status = JobStatus.DONE;
			} else if (work.mode == WorkMode.PIPE) {
				status = JobStatus.DONE;
			} else {
				status = JobStatus.ERROR;
			}
		} catch (TException e) {
			status = JobStatus.ERROR;
		} catch (NotImplemented e) {
			status = JobStatus.ERROR;
		} catch (NotSupported e) {
			status = JobStatus.ERROR;
		}
	}

	public JobStatus getStatus() {
		return status;
	}

}
