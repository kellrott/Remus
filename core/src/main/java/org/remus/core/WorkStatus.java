package org.remus.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import org.remus.thrift.AppletRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkStatus {

	private RemusPipeline pipeline;
	private RemusInstance inst;
	private RemusApplet applet;

	public String startJob;

	public String pathStr;
	public List<String> pathArray;
	public String lPathStr;
	public String rPathStr;
	private Logger logger;

	public static final String WORKDONE_FIELD = "_workdone";
	public static final String JOBSTART_FIELD = "_jobStart";
	public static final String DONECOUNT_FIELD = "_doneCount";
	public static final String ERRORCOUNT_FIELD = "_errorCount";
	public static final String TOTALCOUNT_FIELD = "_totalCount";
	public static final String TIMESTAMP_FIELD = "_timeStamp";

	public static final String WorkStatusName = "/@work";
	public static final String WorkDoneName = "/@done";

	public WorkStatus(RemusPipeline pipeline, RemusInstance inst, RemusApplet applet) {
		this.inst = inst;
		this.applet = applet;
		this.pipeline = pipeline;
		logger = LoggerFactory.getLogger(WorkStatus.class);
	}	

	@Override
	public int hashCode() {
		return inst.hashCode() + applet.hashCode();	
	}

	@Override
	public boolean equals(Object obj) {
		WorkStatus w = (WorkStatus)obj;
		if ( w.inst.equals(inst) && w.applet.equals(applet) ) {
			return true;
		}
		return false;
	}	

	@Override
	public String toString() {
		return inst.toString() + ":" + applet.getID();
	}


	public void setWorkStat( int jobStart, int doneCount, int errorCount, int totalCount, long timestamp) {
		setWorkStat(pipeline, applet, inst, jobStart, doneCount, errorCount, totalCount, timestamp);
	}


	@SuppressWarnings("rawtypes")
	public Collection<Long> getReadyJobs(int count) {
		ArrayList<Long> out = new ArrayList<Long>(count);
		Map status = getStatus();
		Long jobStart = (Long) status.get(JOBSTART_FIELD);
		if ( jobStart == null )
			jobStart = 0L;		
		boolean found = false;
		long newJobStart = jobStart;

		AppletRef arStatus = new AppletRef( pipeline.getID(), inst.toString(), applet.getID() + WorkStatusName );
		AppletRef arDone = new AppletRef( pipeline.getID(), inst.toString(), applet.getID() + WorkDoneName );

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
		} catch (TException e) {
			e.printStackTrace();
		}
		if ( newJobStart != jobStart ) {
			logger.info("JobStart : " + inst.toString() + ":" + applet.getID() + " = " + jobStart );
			status.put(JOBSTART_FIELD, newJobStart);
			setStatus( status );
		}
		if ( count > 0 && out.size() == 0 ) {
			logger.info("Work DONE: " + inst.toString() + ":" + applet.getID() );
			setComplete(pipeline, applet, inst);
		}
		return out;		
	}

	public Object getJob(Long jobID)  {
		Object out = null;
		try {
			AppletRef arWork = new AppletRef( pipeline.getID(), inst.toString(), applet.getID() + WorkStatusName );
			for ( Object val : applet.getDataStore().get( arWork, String.valueOf(jobID)) ) {
				out = val;
			} 
		} catch (TException e) {
			e.printStackTrace();
		}
		return out;
	}

	public void finishJob(long jobID, String workerID) {
		AppletRef arWork = new AppletRef( pipeline.getID(), inst.toString(), applet.getID() + WorkStatusName );
		try {
			applet.getDataStore().add( arWork, 0,0, String.valueOf(jobID), workerID);
		} catch (TException e) {
			e.printStackTrace();
		}
	}



	@SuppressWarnings("rawtypes")
	public Map getStatus() {
		return getStatus( pipeline, applet, inst );
	}

	@SuppressWarnings("rawtypes")
	public static Map getStatus(RemusPipeline pipeline, RemusApplet applet, RemusInstance remusInstance) {
		Object statObj = null;
		AppletRef arWork = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		try {
			for ( Object curObj : applet.getDataStore().get( arWork, remusInstance.toString() ) ) {
				statObj = curObj;
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		return (Map)statObj;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void setWorkStat( RemusPipeline pipeline, RemusApplet applet, RemusInstance inst, int jobStart, int doneCount, int errorCount, int totalCount, long timestamp) {
		Map u = new HashMap();
		u.put(JOBSTART_FIELD, jobStart);
		u.put(DONECOUNT_FIELD, doneCount);
		u.put(ERRORCOUNT_FIELD, errorCount);
		u.put(TOTALCOUNT_FIELD, totalCount);
		u.put(TIMESTAMP_FIELD, timestamp);		
		updateStatus(pipeline, applet, inst, u);
	}



	@SuppressWarnings({"rawtypes" })
	public static boolean isComplete( RemusPipeline pipeline, RemusApplet applet, RemusInstance remusInstance ) {
		boolean found = false;
		AppletRef arStatus = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		AppletRef arError = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@error" );
		try {
			for ( Object statObj : applet.getDataStore().get( arStatus, remusInstance.toString() ) ) {
				if ( statObj != null && ((Map)statObj).containsKey( WorkStatus.WORKDONE_FIELD ) && (Boolean)((Map)statObj).get(WorkStatus.WORKDONE_FIELD) == true ) {
					found = true;
				}
			}
			if ( found ) {
				for ( @SuppressWarnings("unused") String key : applet.getDataStore().listKeys(  arError ) ) {
					found = false;
				}
			}
		} catch (TException e ){
			e.printStackTrace();
		}
		return found;
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void unsetComplete(RemusPipeline pipeline, RemusApplet applet, RemusInstance remusInstance) {
		Object statObj = null;
		AppletRef ar = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() );
		try {
			for ( Object curObj : applet.getDataStore().get( ar, remusInstance.toString() ) ) {
				statObj = curObj;
			}
		} catch (TException e ) {
			e.printStackTrace();
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		//logger.info("UNSET COMPLETE: " + applet.getPath() );
		((Map)statObj).put( WorkStatus.WORKDONE_FIELD, false);
		AppletRef arWork = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		try {
			applet.getDataStore().add( arWork, 0, 0, remusInstance.toString(), statObj );
		} catch (TException e ) {
			e.printStackTrace();
		}
		//datastore.delete( getPath() + "/@done", remusInstance.toString() );
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void setComplete(RemusPipeline pipeline, RemusApplet applet, RemusInstance remusInstance) {
		Object statObj = null;
		AppletRef arWork = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		try {
			for ( Object curObj : applet.getDataStore().get( arWork, remusInstance.toString() ) ) {
				statObj = curObj;
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		//logger.info("SET COMPLETE: " + applet.getPath() );
		((Map)statObj).put( WorkStatus.WORKDONE_FIELD, true);
		try {
			applet.getDataStore().add( arWork, 0, 0, remusInstance.toString(), statObj );
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void updateStatus( RemusPipeline pipeline, RemusApplet applet, RemusInstance inst, Map update ) {
		Object statObj = null;
		AppletRef arWork = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		try {
			for ( Object obj : applet.getDataStore().get( arWork, inst.toString()) ) {
				statObj = obj;
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		for ( Object key : update.keySet() ) {
			((Map)statObj).put(key, update.get(key));
		}
		try {
			applet.getDataStore().add(arWork, 0L, 0L, inst.toString(), statObj );
		} catch (TException e ) {
			e.printStackTrace();
		}
	}

	public void setStatus( Map statObj ) {
		AppletRef arWork = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		try {
			applet.getDataStore().add( arWork, 0L, 0L, inst.toString(), statObj );
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static long getTimeStamp(RemusPipeline pipeline, RemusApplet applet, RemusInstance remusInstance) {
		AppletRef ar = new AppletRef( pipeline.getID(), remusInstance.toString(), applet.getID() );
		AppletRef arDone = new AppletRef( pipeline.getID(), remusInstance.toString(), applet.getID() + "/@done" );

		try {
			long val1 = applet.getDataStore().getTimeStamp(ar);
			long val2 = applet.getDataStore().getTimeStamp(arDone);
			return Math.max(val1, val2);
		} catch (TException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public RemusApplet getApplet() {
		return applet;
	}

	public RemusInstance getInstance() {
		return inst;
	}

	public boolean isComplete() {
		return isComplete(pipeline, applet, inst );
	}

	public static boolean hasStatus(RemusPipeline pipeline, RemusApplet applet, RemusInstance inst) {
		AppletRef ar = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID()+ WorkStatusName );
		try {
			return applet.getDataStore().containsKey( ar, inst.toString() );
		} catch (TException e) {
			e.printStackTrace();
		}
		return false;
	}


}
