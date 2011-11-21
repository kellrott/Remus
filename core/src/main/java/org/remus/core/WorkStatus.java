package org.remus.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;

import org.json.simple.JSONAware;
import org.json.simple.JSONValue;
import org.remus.RemusDB;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;


/**
 * The WorkStatus class represent the internal tracking data used to 
 * keep measure the progress of an applet's work.
 * The lowest avalible key, the work completion, and 
 * error counts, are all kept in the data structure.
 * 
 * @author kellrott
 *
 */

@SuppressWarnings({ "rawtypes", "unchecked" })
public class WorkStatus implements JSONAware {

	public static final String WORKDONE_FIELD = "_workdone";
	public static final String JOBSTART_FIELD = "_jobStart";
	public static final String DONECOUNT_FIELD = "_doneCount";
	public static final String ERRORCOUNT_FIELD = "_errorCount";
	public static final String TOTALCOUNT_FIELD = "_totalCount";
	public static final String TIMESTAMP_FIELD = "_timeStamp";

	Map base;
	public WorkStatus(Map obj) {
		base = obj;
	}	


	public WorkStatus(int jobStart, int doneCount, int errorCount,
			int totalCount, long timestamp) {
		Map u = new HashMap();
		u.put(JOBSTART_FIELD, jobStart);
		u.put(DONECOUNT_FIELD, doneCount);
		u.put(ERRORCOUNT_FIELD, errorCount);
		u.put(TOTALCOUNT_FIELD, totalCount);
		u.put(TIMESTAMP_FIELD, timestamp);	
		base = u;
	}


	@Override
	public String toString() {
		return base.toString();
	}

	public static void unsetComplete(String pipeline, RemusInstance remusInstance, String applet, RemusDB datastore) {
		Object statObj = null;
		AppletRef ar = new AppletRef( pipeline, RemusInstance.STATIC_INSTANCE_STR, applet );
		try {
			for ( Object curObj : datastore.get( ar, remusInstance.toString() ) ) {
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
		//logger.info("UNSET COMPLETE: " + applet.getPath() );
		((Map)statObj).put( WorkStatus.WORKDONE_FIELD, false);
		AppletRef arWork = new AppletRef( pipeline, RemusInstance.STATIC_INSTANCE_STR, Constants.WORK_APPLET );
		try {
			datastore.add(arWork, 0, 0, remusInstance.toString() + ":" + applet, statObj);
		} catch (TException e ) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}

	public static void setComplete(RemusPipeline pipeline, RemusApplet applet, RemusInstance remusInstance) {
		Object statObj = null;
		AppletRef arWork = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, Constants.WORK_APPLET );
		try {
			for (Object curObj : applet.getDataStore().get( arWork, remusInstance.toString() + ":" + applet.getID())) {
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
		//logger.info("SET COMPLETE: " + applet.getPath() );
		((Map)statObj).put( WorkStatus.WORKDONE_FIELD, true);
		try {
			applet.getDataStore().add(arWork, 0, 0, remusInstance.toString(), statObj);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}

	public static void updateStatus( RemusPipeline pipeline, RemusApplet applet, RemusInstance inst, Map update ) {
		Object statObj = null;
		AppletRef arWork = new AppletRef(pipeline.getID(), 
				RemusInstance.STATIC_INSTANCE_STR, 
				Constants.WORK_APPLET);
		try {
			for (Object obj : applet.getDataStore().get(arWork, inst.toString() + ":" + applet.getID())) {
				statObj = obj;
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		if (statObj == null) {
			statObj = new HashMap();
		}
		for (Object key : update.keySet()) {
			((Map) statObj).put(key, update.get(key));
		}
		try {
			applet.getDataStore().add(arWork, 0L, 0L, inst.toString(), statObj);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}

	public static boolean hasStatus(String pipeline, RemusInstance inst, String applet, RemusDB datastore) {
		AppletRef ar = new AppletRef( pipeline, RemusInstance.STATIC_INSTANCE_STR, 
				Constants.WORK_APPLET);
		try {
			return datastore.containsKey(ar, inst.toString() + ":" + applet);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}


	public String getJobStart() {
		if (base.containsKey(JOBSTART_FIELD)) {
			return base.get(JOBSTART_FIELD).toString();
		} else {
			return "";
		}
			
	}


	public boolean done() {
		if ( base.containsKey( WorkStatus.WORKDONE_FIELD ) && ((Boolean)(base.get(WorkStatus.WORKDONE_FIELD))) == true) {
			return true;
		}
		return false;
	}

	
	public void setJobStart(String newJobStart) {
		base.put(JOBSTART_FIELD, newJobStart);
	}

	@Override
	public String toJSONString() {
		return JSONValue.toJSONString(base);
	}


	public void setWorkDone(boolean b) {
		base.put(WORKDONE_FIELD, b);
	}


	public boolean workCountSet() {
		return base.containsKey(TOTALCOUNT_FIELD);
	}

}
