package org.remus.manage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.remus.RemusApplet;
import org.remus.RemusInstance;
import org.remus.WorkStatus;
import org.remus.work.RemusAppletImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkStatusImpl implements WorkStatus {
	private RemusInstance inst;
	private RemusAppletImpl applet;

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

	public WorkStatusImpl(RemusInstance inst, RemusAppletImpl applet) {
		this.inst = inst;
		this.applet = applet;
	    logger = LoggerFactory.getLogger(WorkStatusImpl.class);

	}	
	
	@Override
	public int hashCode() {
		return inst.hashCode() + applet.hashCode();	
	}
	
	@Override
	public boolean equals(Object obj) {
		WorkStatusImpl w = (WorkStatusImpl)obj;
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
		setWorkStat(applet, inst, jobStart, doneCount, errorCount, totalCount, timestamp);
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
		for ( String key : applet.getDataStore().keySlice( applet.getPath() + WorkStatusName, inst.toString(), String.valueOf(jobStart), count + 1) ) {
			if ( ! applet.getDataStore().containsKey( applet.getPath() + WorkDoneName, inst.toString(), key) ) {
				out.add( Long.valueOf( key ) );
				found = true;
			} else {
				if (!found) {
					newJobStart = Long.valueOf(key);
				}
			}
		}
		if ( newJobStart != jobStart ) {
			logger.info("JobStart : " + inst.toString() + ":" + applet.getID() + " = " + jobStart );
			status.put(JOBSTART_FIELD, newJobStart);
			setStatus( status );
		}
		if ( count > 0 && out.size() == 0 ) {
			logger.info("Work DONE: " + inst.toString() + ":" + applet.getID() );
			setComplete(applet, inst);
		}
		return out;		
	}
	
	
	public Object getJob(Long jobID) {
		Object out = null;
		for ( Object val : applet.getDataStore().get(applet.getPath() + WorkStatusName, inst.toString(), String.valueOf(jobID)) ) {
			out = val;
		}
		return out;
	}
	
	public void finishJob(long jobID, String workerID) {
		applet.getDataStore().add( applet.getPath() + WorkDoneName, inst.toString(), 0,0, String.valueOf(jobID), workerID);
	}

	
	
	@SuppressWarnings("rawtypes")
	public Map getStatus() {
		return getStatus( applet, inst );
	}
	
	@SuppressWarnings("rawtypes")
	public static Map getStatus(RemusAppletImpl applet, RemusInstance remusInstance) {
		Object statObj = null;
		for ( Object curObj : applet.getDataStore().get( applet.getPath() + WorkStatusImpl.WorkStatusName, RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) ) {
			statObj = curObj;
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		return (Map)statObj;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void setWorkStat( RemusAppletImpl applet, RemusInstance inst, int jobStart, int doneCount, int errorCount, int totalCount, long timestamp) {
		Map u = new HashMap();
		u.put(JOBSTART_FIELD, jobStart);
		u.put(DONECOUNT_FIELD, doneCount);
		u.put(ERRORCOUNT_FIELD, errorCount);
		u.put(TOTALCOUNT_FIELD, totalCount);
		u.put(TIMESTAMP_FIELD, timestamp);		
		updateStatus(applet, inst, u);
	}

	 
	
	@SuppressWarnings({"rawtypes" })
	public static boolean isComplete( RemusAppletImpl applet, RemusInstance remusInstance ) {
		boolean found = false;
		for ( Object statObj : applet.getDataStore().get( applet.getPath() + WorkStatusImpl.WorkStatusName, RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) ) {
			if ( statObj != null && ((Map)statObj).containsKey( WorkStatusImpl.WORKDONE_FIELD ) && (Boolean)((Map)statObj).get(WorkStatusImpl.WORKDONE_FIELD) == true ) {
				found = true;
			}
		}
		if ( found ) {
			for ( @SuppressWarnings("unused") String key : applet.getDataStore().listKeys(  applet.getPath() + "/@error", remusInstance.toString() ) ) {
				found = false;
			}
		}
		return found;
	}
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void unsetComplete(RemusAppletImpl applet, RemusInstance remusInstance) {
		Object statObj = null;
		for ( Object curObj : applet.getDataStore().get( applet.getPath() + WorkStatusImpl.WorkStatusName, RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) ) {
			statObj = curObj;
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		//logger.info("UNSET COMPLETE: " + applet.getPath() );
		((Map)statObj).put( WorkStatusImpl.WORKDONE_FIELD, false);
		applet.getDataStore().add( applet.getPath() + WorkStatusImpl.WorkStatusName, RemusInstance.STATIC_INSTANCE_STR, 0, 0, remusInstance.toString(), statObj );
		//datastore.delete( getPath() + "/@done", remusInstance.toString() );
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void setComplete(RemusAppletImpl applet, RemusInstance remusInstance) {
		Object statObj = null;
		for ( Object curObj : applet.getDataStore().get( applet.getPath() + WorkStatusImpl.WorkStatusName, RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) ) {
			statObj = curObj;
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		//logger.info("SET COMPLETE: " + applet.getPath() );
		((Map)statObj).put( WorkStatusImpl.WORKDONE_FIELD, true);
		applet.getDataStore().add( applet.getPath() + WorkStatusImpl.WorkStatusName, RemusInstance.STATIC_INSTANCE_STR, 0, 0, remusInstance.toString(), statObj );
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void updateStatus( RemusAppletImpl applet, RemusInstance inst, Map update ) {
		Object statObj = null;
		for ( Object obj : applet.getDataStore().get( applet.getPath() + WorkStatusImpl.WorkStatusName , RemusInstance.STATIC_INSTANCE_STR, inst.toString()) ) {
			statObj = obj;
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		for ( Object key : update.keySet() ) {
			((Map)statObj).put(key, update.get(key));
		}
		applet.getDataStore().add( applet.getPath() + WorkStatusImpl.WorkStatusName, RemusInstance.STATIC_INSTANCE_STR, 0L, 0L, inst.toString(), statObj );
	}
	
	public void setStatus( Map statObj ) {
		applet.getDataStore().add( applet.getPath() + WorkStatusImpl.WorkStatusName, RemusInstance.STATIC_INSTANCE_STR, 0L, 0L, inst.toString(), statObj );
	}

	public static long getTimeStamp(RemusAppletImpl applet, RemusInstance remusInstance) {
		long val1 = applet.getDataStore().getTimeStamp( applet.getPath(), remusInstance.toString());
		long val2 = applet.getDataStore().getTimeStamp( applet.getPath() + "/@done" , remusInstance.toString());
		return Math.max(val1, val2);
	}

	public RemusAppletImpl getApplet() {
		return applet;
	}

	public RemusInstance getInstance() {
		return inst;
	}

	public boolean isComplete() {
		return isComplete(applet, inst );
	}

	public static boolean hasStatus(RemusAppletImpl applet, RemusInstance inst) {
		return applet.getDataStore().containsKey( applet.getPath() + WorkStatusImpl.WorkStatusName, RemusInstance.STATIC_INSTANCE_STR, inst.toString() );
	}

	
}
