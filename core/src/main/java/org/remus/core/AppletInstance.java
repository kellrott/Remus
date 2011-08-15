package org.remus.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
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

	public static final String WorkStatusName = "/@work";
	public static final String WorkDoneName = "/@done";


	RemusPipeline pipeline;
	RemusApplet applet;
	RemusInstance instance;
	RemusDB datastore;
	private Logger logger;
	public AppletInstance( RemusPipeline pipeline, RemusInstance instance, RemusApplet applet, RemusDB database ) {
		logger = LoggerFactory.getLogger(AppletInstance.class);
		this.pipeline = pipeline;
		this.instance = instance;
		this.applet = applet;
		this.datastore = database;
	}

	@Override
	public int hashCode() {
		return instance.hashCode() + applet.hashCode();	
	}

	@Override
	public boolean equals(Object obj) {
		AppletInstance w = (AppletInstance) obj;
		if ( w.instance.equals(instance) && w.applet.equals(applet) ) {
			return true;
		}
		return false;
	}	

	public boolean isReady( ) {
		if ( applet.getMode() == RemusApplet.STORE )
			return true;
		if ( applet.hasInputs() ) {
			boolean allReady = true;
			for ( String iRef : applet.getInputs() ) {
				if ( iRef.compareTo("?") != 0 ) {
					try {
						RemusApplet iApplet = pipeline.getApplet( iRef );
						if ( iApplet != null ) {
							if ( iApplet.getMode() != RemusApplet.STORE ) {
								AppletInstance ai = new AppletInstance(pipeline, instance, iApplet, datastore);
								if ( ! ai.isComplete() ) {
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
		for ( String iRef : applet.getInputs() ) {
			if ( iRef.compareTo("?") != 0 ) {
				try {
					RemusApplet iApplet = pipeline.getApplet(iRef);
					if ( iApplet != null ) {
						try {
							AppletInstance ai = new AppletInstance(pipeline, instance, iApplet, datastore);
							long val = ai.getDataTimeStamp();
							if ( out < val ) {
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
		return datastore.getTimeStamp( new AppletRef(pipeline.getID(), instance.toString(), applet.getID() ) );
	}

	public long getStatusTimeStamp() throws NotImplemented, TException {
		return datastore.getTimeStamp( new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + WorkStatusName ) );
	}


	public boolean isInError(  RemusInstance remusInstance ) {
		boolean found = false;
		AppletRef ar = new AppletRef(pipeline.getID(), remusInstance.toString(), applet.getID() + "/@error" );

		for ( @SuppressWarnings("unused") String key : datastore.listKeys( ar ) ) {
			found = true;
		}
		return found;
	}




	public void finishWork(RemusInstance remusInstance, long jobID, String workerName, long emitCount) throws TException, NotImplemented {
		AppletRef ar = new AppletRef(pipeline.getID(), remusInstance.toString(), applet.getID() + "/@done" );
		datastore.add(ar, 0L, 0L, Long.toString(jobID), workerName);
	}

	public void errorJob(long jobID, String errorMsg) {
		AppletRef arWork = new AppletRef(pipeline.getID(), instance.toString(), applet.getID() + "/@error");
		try {
			applet.getDataStore().add(arWork, 0, 0,
					String.valueOf(jobID), errorMsg);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}

	public Collection<Long> getReadyJobs(int count) {
		ArrayList<Long> out = new ArrayList<Long>(count);
		WorkStatus status = getStatus();

		Long jobStart = status.getJobStart();
		if ( jobStart == null )
			jobStart = 0L;		
		boolean found = false;
		long newJobStart = jobStart;

		AppletRef arStatus = new AppletRef( pipeline.getID(), instance.toString(), applet.getID() + WorkStatusName );
		AppletRef arDone = new AppletRef( pipeline.getID(), instance.toString(), applet.getID() + WorkDoneName );

		try {
			for ( String key : applet.getDataStore().keySlice( arStatus, String.valueOf(jobStart), count + 1) ) {
				if ( ! applet.getDataStore().containsKey( arDone, key) ) {
					out.add( Long.valueOf( key ) );
					found = true;
				} else {
					if (!found) {
						newJobStart = Long.valueOf(key);
					}
				}
			}
			if (newJobStart != jobStart) {
				logger.info("JobStart : " + instance.toString() + ":" + applet.getID() + " = " + jobStart );
				status.setJobStart(newJobStart);
				setWorkStat( status );
			}
			if ( count > 0 && out.size() == 0 ) {
				logger.info("Work DONE: " + instance.toString() + ":" + applet.getID() );
				setComplete();
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}

		return out;		
	}

	public void setComplete() {
		// TODO Auto-generated method stub

	}

	public void setWorkStat(WorkStatus status) throws TException, NotImplemented {
		AppletRef arWork = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@work" );
		datastore.add(arWork, 0, 0, instance.toString(), status);
	}

	public void setWorkStat( int jobStart, int doneCount, int errorCount, int totalCount, long timestamp) throws TException, NotImplemented {
		WorkStatus ws = new WorkStatus(jobStart, doneCount, errorCount, totalCount, timestamp);
		setWorkStat(ws);
	}

	@SuppressWarnings("rawtypes")
	public WorkStatus getStatus() {
		Object statObj = null;
		AppletRef arWork = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@work" );
		try {
			for ( Object curObj : applet.getDataStore().get( arWork, instance.toString() ) ) {
				statObj = curObj;
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		return new WorkStatus((Map)statObj);
	}

	/*

	@SuppressWarnings({"rawtypes" })
	public static boolean isComplete( RemusPipeline pipeline, RemusApplet applet, RemusInstance remusInstance ) {
		boolean found = false;
		AppletRef arStatus = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@work" );
		AppletRef arError = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@error" );
		try {
			for (Object statObj : applet.getDataStore().get( arStatus, remusInstance.toString())) {
				if ( statObj != null && ((Map)statObj).containsKey( WorkStatus.WORKDONE_FIELD ) && (Boolean)((Map)statObj).get(WorkStatus.WORKDONE_FIELD) == true ) {
					found = true;
				}
			}
			if ( found ) {
				for (@SuppressWarnings("unused") String key : applet.getDataStore().listKeys(  arError ) ) {
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
	 */
	@SuppressWarnings("rawtypes")
	public boolean isComplete() {
		boolean found = false;
		AppletRef arStatus = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@work" );
		AppletRef arError = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@error" );
		try {
			for (Object statObj : applet.getDataStore().get( arStatus, instance.toString())) {
				if (statObj != null) {
					WorkStatus ws = new WorkStatus((Map)statObj);
					if ( ws.done() ) {
						found = true;
					}
				}
			}
			if ( found ) {
				for (@SuppressWarnings("unused") String key : applet.getDataStore().listKeys(  arError ) ) {
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

	public RemusApplet getPipeline() {
		return applet;
	}

	public RemusInstance getInstance() {
		return instance;
	}

	public RemusApplet getApplet() {
		return applet;
	}
	
	public PipelineSubmission getInstanceInfo() throws TException, NotImplemented {
		AppletRef ar = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@instance" );		
		for (Object obj : datastore.get(ar, instance.toString())) {
			return new PipelineSubmission(obj);
		}
		return null;
	}

	public void updateInstanceInfo(PipelineSubmission instInfo) throws TException, NotImplemented {
		AppletRef ar = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@instance" );		
		datastore.add(ar, 0, 0, instance.toString(), instInfo);		
	}



}
