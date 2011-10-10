package org.remus.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;
import org.remus.work.WorkGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AppletInstance refers to a specific Remus Applet instance. This class
 * can be used to determine is an applet instance has complete, is ready,
 * has old timestamps, and determine which jobs are ready to be worked on.
 * @author kellrott
 *
 */
public class AppletInstance {
	RemusPipeline pipeline;
	RemusApplet applet;
	RemusInstance instance;
	RemusDB datastore;
	private Logger logger;
	
	public AppletInstance(RemusPipeline pipeline, RemusInstance instance, RemusApplet applet, RemusDB datastore) {
		logger = LoggerFactory.getLogger(AppletInstance.class);
		this.pipeline = pipeline;
		this.instance = instance;
		this.applet = applet;
		this.datastore = datastore;
	}

	
	@Override
	public String toString() {
		return instance.toString() + ":" + applet.getID();
	}
	
	@Override
	public int hashCode() {
		return instance.hashCode() + applet.hashCode();	
	}

	@Override
	public boolean equals(Object obj) {
		AppletInstance w = (AppletInstance) obj;
		if (w.instance.equals(instance) && w.applet.equals(applet)) {
			return true;
		}
		return false;
	}	

	public boolean isReady( ) {
		if (applet.getMode() == RemusApplet.STORE) {
			return true;
		}
		if (isInError()) {
			return false;
		}
		if (applet.hasSources()) {
			boolean allReady = true;
			for (String iRef : applet.getSources()) {
				if (iRef.compareTo("?") != 0) {
					try {
						RemusApplet iApplet = pipeline.getApplet(iRef);
						if (iApplet != null) {
							if (iApplet.getMode() == RemusApplet.OUTPUT) {
								iApplet = pipeline.getApplet(iRef.split(":")[0]);
							}
							if (iApplet.getMode() != RemusApplet.STORE) {
								AppletInstance ai = new AppletInstance(pipeline, instance, iApplet, datastore);
								if (!ai.isComplete()) {
									allReady = false;
								}
							}
						} else {
							allReady = false;
						}
					} catch (RemusDatabaseException e) {
						e.printStackTrace();
						allReady = false;
					}
				} else {				
					allReady = true;
				}
			}
			return allReady;
		}		
		return true;
	}



	public long inputTimeStamp( ) {
		long out = 0;
		for (String iRef : applet.getSources()) {
			if (iRef.compareTo("?") != 0) {
				try {
					RemusApplet iApplet = pipeline.getApplet(iRef);
					if (iApplet != null) {
						try {
							AppletInstance ai = new AppletInstance(pipeline, instance, iApplet, datastore);
							//long val = ai.getDataTimeStamp();
							long val = ai.getStatusTimeStamp();
							if (out < val) {
								out = val;
							}
						} catch (TException e) {
							e.printStackTrace();
						} catch (NotImplemented e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} catch (RemusDatabaseException e) {
					e.printStackTrace();
				}
			}			
		}
		return out;
	}


	public long getDataTimeStamp( ) throws TException, NotImplemented {
		return datastore.getTimeStamp(new AppletRef(pipeline.getID(), instance.toString(), applet.getID()));
	}

	public long getStatusTimeStamp() throws NotImplemented, TException {
		return datastore.getTimeStamp(new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + Constants.WORK_APPLET));
	}


	public boolean isInError() {
		boolean found = false;
		AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + Constants.ERROR_APPLET);

		for (@SuppressWarnings("unused") String key : datastore.listKeys(ar)) {
			found = true;
		}
		return found;
	}




	public void finishWork(long jobID, String workerName, long emitCount) throws TException, NotImplemented {
		AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + Constants.DONE_APPLET);
		datastore.add(ar, 0L, 0L, Long.toString(jobID), workerName);
		if (applet.workGenerator != null) {
			try {
				WorkGenerator gen = (WorkGenerator) applet.workGenerator.newInstance();
				gen.finalizeWork(pipeline, applet, instance, datastore);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void errorWork(long jobID, String errorMsg) {
		AppletRef arWork = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + Constants.ERROR_APPLET);
		try {
			applet.getDataStore().add(arWork, 0, 0,
					String.valueOf(jobID), errorMsg);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}

	public long [] getReadyJobs(int count) {
		long [] out = new long[count];
		int curPos = 0;
		WorkStatus status = getStatus();

		String jobStart = status.getJobStart();
		if (jobStart == null) {
			jobStart = "";
		}
		String newJobStart = jobStart;

		AppletRef arStatus = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + Constants.WORK_APPLET);
		AppletRef arDone = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + Constants.DONE_APPLET);

		try {
			boolean found = true;
			boolean allDone = true;
			boolean firstSlice = true;
			String sliceStart = jobStart;
			logger.debug("Starting JobScan: " + instance.toString() + ":" + applet.getID() + " " + jobStart );
			while (found && curPos < count) {
				found = false;
				for (String key : applet.getDataStore().keySlice(arStatus, sliceStart, count - curPos)) {
					if (firstSlice || key.compareTo(sliceStart) != 0) {
						found = true;
						if (!applet.getDataStore().containsKey(arDone, key)) {
							out[curPos] = Long.valueOf(key);
							curPos++;
						} else {
							if (allDone) {
								newJobStart = key;
							}
							allDone = false;
						}
						sliceStart = key;
						firstSlice = false;
					}
				}
			}
			logger.debug("Ending JobScan: " + instance.toString() + ":" + applet.getID() + " " + newJobStart );
			if (count > 0 && curPos == 0) {
				logger.info("Work DONE: " + instance.toString() + ":" + applet.getID());
				setComplete();
			} else if (newJobStart.equals(jobStart)) {
				logger.info("JobStart : " + instance.toString() + ":" + applet.getID() + " = " + jobStart);
				status.setJobStart(newJobStart);
				setWorkStat(status);
			}

			//logger.debug("Found : " + curPos + " jobs");
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}

		long [] a = Arrays.copyOf(out, curPos);
		Arrays.sort(a);
		return a;
	}


	public void setComplete() {
		WorkStatus stat = getStatus();
		stat.setWorkDone(true);
		try {
			setWorkStat(stat);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setWorkStat(WorkStatus status) throws TException, NotImplemented {
		AppletRef arWork = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + Constants.WORK_APPLET);
		datastore.add(arWork, 0, 0, instance.toString(), status);
	}

	public void setWorkStat( int jobStart, int doneCount, int errorCount, int totalCount, long timestamp) throws TException, NotImplemented {
		WorkStatus ws = new WorkStatus(jobStart, doneCount, errorCount, totalCount, timestamp);
		setWorkStat(ws);
	}

	@SuppressWarnings("rawtypes")
	public WorkStatus getStatus() {
		Object statObj = null;
		AppletRef arWork = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + Constants.WORK_APPLET);
		try {
			for (Object curObj : applet.getDataStore().get(arWork, instance.toString())) {
				statObj = curObj;
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		if (statObj == null) {
			statObj = new HashMap();
		}
		return new WorkStatus((Map) statObj);
	}

	
	@SuppressWarnings("rawtypes")
	public boolean isComplete() {
		boolean found = false;
		AppletRef arStatus = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + Constants.WORK_APPLET);
		AppletRef arError = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + Constants.ERROR_APPLET);
		try {
			for (Object statObj : applet.getDataStore().get(arStatus, instance.toString())) {
				if (statObj != null) {
					WorkStatus ws = new WorkStatus((Map) statObj);
					if (ws.done()) {
						found = true;
					}
				}
			}
			if (found) {
				for (@SuppressWarnings("unused") String key : applet.getDataStore().listKeys(arError)) {
					found = false;
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		return found;
	}

	public RemusPipeline getPipeline() {
		return pipeline;
	}

	public RemusInstance getInstance() {
		return instance;
	}

	public RemusApplet getApplet() {
		return applet;
	}

	public PipelineSubmission getInstanceInfo() throws TException, NotImplemented {
		AppletRef ar = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + Constants.INSTANCE_APPLET);
		for (Object obj : datastore.get(ar, instance.toString())) {
			return new PipelineSubmission(obj);
		}
		return null;
	}

	public void updateInstanceInfo(PipelineSubmission instInfo) throws TException, NotImplemented {
		AppletRef ar = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + Constants.INSTANCE_APPLET);
		datastore.add(ar, 0, 0, instance.toString(), instInfo);		
	}

	public AppletRef getAppletRef() {
		AppletRef arWork = new AppletRef(pipeline.getID(), instance.toString(), applet.getID());
		return arWork;
	}
	
}
