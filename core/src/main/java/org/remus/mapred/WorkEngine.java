package org.remus.mapred;

import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusInstance;
import org.remus.thrift.AppletRef;
import org.remus.thrift.JobState;
import org.remus.thrift.JobStatus;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.WorkMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkEngine implements Runnable {
	private WorkDesc work;
	private RemusDB db;
	private MapReduceFunction mapred;
	private JobState status;
	private RemusAttach attach;
	private Logger logger;
	public WorkEngine(WorkDesc work, RemusDB db, RemusAttach attach, MapReduceFunction mapred) {
		this.work = work;
		this.db = db;
		this.attach = attach;
		this.mapred = mapred;
		status = JobState.QUEUED;
		logger = LoggerFactory.getLogger(WorkEngine.class);
	}

	@Override
	public void run() {
		AppletRef arWork = new AppletRef(work.workStack.pipeline, 
				work.workStack.instance, work.workStack.applet + "/@work");
		status = JobState.WORKING;

		Map stackInfo = (Map) JSON.loads(work.infoJSON);
		mapred.init(stackInfo);
		PipelineSubmission sub = new PipelineSubmission(stackInfo);
		try { 
			if (work.mode == WorkMode.MAP) {
				String inputInstStr = sub.getInputInstance();				
				RemusInstance inst = RemusInstance.getInstance(db, 
						work.workStack.pipeline, 
						inputInstStr);
				AppletRef ar = new AppletRef(
						work.workStack.pipeline,
						inst.toString(),
						sub.getInputApplet()
						);
				AppletRef outRef = new AppletRef(
						work.workStack.pipeline,
						work.workStack.instance,
						work.workStack.applet
						);
				for (long jobID = work.workStart; jobID < work.workEnd; jobID++) {
					for (Object key : db.get(arWork, Long.toString(jobID))) {
						MapReduceCallback cb = new MapReduceCallback(work.workStack.pipeline, work.workStack.applet, stackInfo, db, attach);
						for (Object value : db.get(ar, (String) key)) {
							mapred.map((String) key, value, cb);
						}
						cb.writeEmits(outRef, jobID);
						logger.info("Emiting: " + cb.emitCount);
					}
				}
				status = JobState.DONE;
			} else if (work.mode == WorkMode.SPLIT) {
				MapReduceCallback cb = new MapReduceCallback(work.workStack.pipeline, work.workStack.applet, stackInfo, db, attach);

				AppletRef outRef = new AppletRef(
						work.workStack.pipeline,
						work.workStack.instance,
						work.workStack.applet
						);
				
				mapred.split(stackInfo, cb);
				cb.writeEmits(outRef, 0);
				logger.info("Emiting: " + cb.emitCount);

				status = JobState.DONE;
			} else if (work.mode == WorkMode.REDUCE) {
				String inputInstStr = sub.getInputInstance();				
				RemusInstance inst = RemusInstance.getInstance(db, 
						work.workStack.pipeline, 
						inputInstStr);
				AppletRef ar = new AppletRef(
						work.workStack.pipeline,
						inst.toString(),
						sub.getInputApplet()
						);
				AppletRef outRef = new AppletRef(
						work.workStack.pipeline,
						work.workStack.instance,
						work.workStack.applet
						);
				
				for (long jobID = work.workStart; jobID < work.workEnd; jobID++) {
					MapReduceCallback cb = new MapReduceCallback(work.workStack.pipeline, work.workStack.applet, stackInfo, db, attach);
					for (Object key : db.get(arWork, Long.toString(jobID))) {
						mapred.reduce((String) key, db.get(ar, (String) key), cb);
					}
					cb.writeEmits(outRef, jobID);
					logger.info("Emiting: " + cb.emitCount);
				}
				status = JobState.DONE;
			} else if (work.mode == WorkMode.MATCH) {
				status = JobState.DONE;
			} else if (work.mode == WorkMode.MERGE) {
				status = JobState.DONE;
			} else if (work.mode == WorkMode.PIPE) {
				status = JobState.DONE;
			} else {
				status = JobState.ERROR;
			}
		} catch (TException e) {
			status = JobState.ERROR;
		} catch (NotImplemented e) {
			status = JobState.ERROR;
		} catch (NotSupported e) {
			status = JobState.ERROR;
		}
	}

	public JobStatus getStatus() {
		JobStatus out = new JobStatus();
		out.status = status;
		return out;
	}

}
