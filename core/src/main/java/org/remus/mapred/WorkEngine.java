package org.remus.mapred;

import org.apache.thrift.TException;
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
		AppletRef ar = new AppletRef(work.input.pipeline, 
				work.input.instance, work.input.applet);
		AppletRef arWork = new AppletRef(work.input.pipeline, 
				work.input.instance, work.input.applet + "/@work");
		status = JobStatus.WORKING;

		try { 
			if (work.mode == WorkMode.MAP) {
				for (long jobID : work.jobs) {
					for (Object key : db.get(arWork, Long.toString(jobID))) {
						MapReduceCallback cb = new MapReduceCallback();
						for (Object value : db.get(ar, (String) key)) {
							mapred.map((String) key, value, cb);
						}
						
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
