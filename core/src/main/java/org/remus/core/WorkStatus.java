package org.remus.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;

import org.json.simple.JSONAware;
import org.json.simple.JSONValue;
import org.remus.thrift.AppletRef;
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

	public static final String WorkStatusName = "/@work";
	public static final String WorkDoneName = "/@done";
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

	public static void unsetComplete(RemusPipeline pipeline, RemusApplet applet, RemusInstance remusInstance) {
		Object statObj = null;
		AppletRef ar = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() );
		try {
			for ( Object curObj : applet.getDataStore().get( ar, remusInstance.toString() ) ) {
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
		AppletRef arWork = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		try {
			applet.getDataStore().add(arWork, 0, 0, remusInstance.toString(), statObj);
		} catch (TException e ) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		//datastore.delete( getPath() + "/@done", remusInstance.toString() );
	}

	public static void setComplete(RemusPipeline pipeline, RemusApplet applet, RemusInstance remusInstance) {
		Object statObj = null;
		AppletRef arWork = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		try {
			for (Object curObj : applet.getDataStore().get( arWork, remusInstance.toString())) {
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
				RemusInstance.STATIC_INSTANCE_STR, applet.getID() 
				+ WorkStatusName);
		try {
			for (Object obj : applet.getDataStore().get(arWork, inst.toString())) {
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
		} catch (TException e ) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}

	/*
	public void setStatus( Map statObj ) {
		AppletRef arWork = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + WorkStatusName );
		try {
			applet.getDataStore().add( arWork, 0L, 0L, inst.toString(), statObj );
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}
	 */
	/*
	public static long getTimeStamp(RemusPipeline pipeline, RemusApplet applet, RemusInstance remusInstance) {
		AppletRef ar = new AppletRef( pipeline.getID(), remusInstance.toString(), applet.getID() );
		AppletRef arDone = new AppletRef( pipeline.getID(), remusInstance.toString(), applet.getID() + "/@done" );

		try {
			long val1 = applet.getDataStore().getTimeStamp(ar);
			long val2 = applet.getDataStore().getTimeStamp(arDone);
			return Math.max(val1, val2);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		return 0;
	}
	 */

	//public boolean isComplete() {
	//	return isComplete(pipeline, applet, inst );
	//}

	public static boolean hasStatus(RemusPipeline pipeline, RemusApplet applet, RemusInstance inst) {
		AppletRef ar = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID()+ WorkStatusName );
		try {
			return applet.getDataStore().containsKey( ar, inst.toString() );
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}


	public Long getJobStart() {
		return (Long)base.get(JOBSTART_FIELD);
	}


	public boolean done() {
		if ( base.containsKey( WorkStatus.WORKDONE_FIELD ) && ((Boolean)(base.get(WorkStatus.WORKDONE_FIELD))) == true) {
			return true;
		}
		return false;
	}

	
	public void setJobStart(long newJobStart) {
		base.put(JOBSTART_FIELD, newJobStart);
	}

	@Override
	public String toJSONString() {
		return JSONValue.toJSONString(base);
	}

}
