package org.remus.manage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.RemusApplet;
import org.remus.RemusInstance;
import org.remus.WorkStatus;
import org.remus.work.RemusAppletImpl;
import org.remusNet.thrift.AppletRef;
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
	@Override
	public Collection<Long> getReadyJobs(int count) {
		ArrayList<Long> out = new ArrayList<Long>(count);
		Map status = getStatus();
		Long jobStart = (Long) status.get(JOBSTART_FIELD);
		if ( jobStart == null )
			jobStart = 0L;		
		boolean found = false;
		long newJobStart = jobStart;

		AppletRef arStatus = new AppletRef( applet.getPipeline().getID(), inst.toString(), applet.getID() + WorkStatusName );
		AppletRef arDone = new AppletRef( applet.getPipeline().getID(), inst.toString(), applet.getID() + WorkDoneName );

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
			setComplete(applet, inst);
		}
		return out;		
	}

	@Override
	public Object getJob(Long jobID)  {
		Object out = null;
		try {
			AppletRef arWork = new AppletRef( applet.getPipeline().getID(), inst.toString(), applet.getID() + WorkStatusName );
			for ( Object val : applet.getDataStore().get( arWork, String.valueOf(jobID)) ) {
				out = val;
			} 
		} catch (TException e) {
			e.printStackTrace();
		}
		return out;
	}

	public void finishJob(long jobID, String workerID) {
		AppletRef arWork = new AppletRef( applet.getPipeline().getID(), inst.toString(), applet.getID() + WorkStatusName );
		try {
			applet.getDataStore().add( arWork, 0,0, String.valueOf(jobID), workerID);
		} catch (TException e) {
			e.printStackTrace();
		}
	}



	@SuppressWarnings("rawtypes")
	public Map getStatus() {
		return getStatus( applet, inst );
	}

	@SuppressWarnings("rawtypes")
	public static Map getStatus(RemusAppletImpl applet, RemusInstance remusInstance) {
		Object statObj = null;
		AppletRef arWork = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
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
		AppletRef arStatus = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		AppletRef arError = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@error" );
		try {
			for ( Object statObj : applet.getDataStore().get( arStatus, remusInstance.toString() ) ) {
				if ( statObj != null && ((Map)statObj).containsKey( WorkStatusImpl.WORKDONE_FIELD ) && (Boolean)((Map)statObj).get(WorkStatusImpl.WORKDONE_FIELD) == true ) {
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
	public static void unsetComplete(RemusAppletImpl applet, RemusInstance remusInstance) {
		Object statObj = null;
		AppletRef ar = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() );
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
		((Map)statObj).put( WorkStatusImpl.WORKDONE_FIELD, false);
		AppletRef arWork = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		try {
			applet.getDataStore().add( arWork, 0, 0, remusInstance.toString(), statObj );
		} catch (TException e ) {
			e.printStackTrace();
		}
		//datastore.delete( getPath() + "/@done", remusInstance.toString() );
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void setComplete(RemusAppletImpl applet, RemusInstance remusInstance) {
		Object statObj = null;
		AppletRef arWork = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
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
		((Map)statObj).put( WorkStatusImpl.WORKDONE_FIELD, true);
		try {
			applet.getDataStore().add( arWork, 0, 0, remusInstance.toString(), statObj );
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void updateStatus( RemusAppletImpl applet, RemusInstance inst, Map update ) {
		Object statObj = null;
		AppletRef arWork = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
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
		AppletRef arWork = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		try {
			applet.getDataStore().add( arWork, 0L, 0L, inst.toString(), statObj );
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static long getTimeStamp(RemusAppletImpl applet, RemusInstance remusInstance) {
		AppletRef ar = new AppletRef( applet.getPipeline().getID(), remusInstance.toString(), applet.getID() );
		AppletRef arDone = new AppletRef( applet.getPipeline().getID(), remusInstance.toString(), applet.getID() + "/@done" );

		try {
			long val1 = applet.getDataStore().getTimeStamp(ar);
			long val2 = applet.getDataStore().getTimeStamp(arDone);
			return Math.max(val1, val2);
		} catch (TException e) {
			e.printStackTrace();
		}
		return 0;
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
		AppletRef ar = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID()+ WorkStatusName );
		try {
			return applet.getDataStore().containsKey( ar, inst.toString() );
		} catch (TException e) {
			e.printStackTrace();
		}
		return false;
	}


}
