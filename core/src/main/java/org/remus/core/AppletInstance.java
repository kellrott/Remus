package org.remus.core;

import java.util.Map;

import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.server.RemusDatabaseException;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class AppletInstance {

	RemusPipeline pipeline;
	RemusApplet applet;
	RemusInstance instance;
	RemusDB datastore;
	public AppletInstance( RemusPipeline pipeline, RemusInstance instance, RemusApplet applet, RemusDB database ) {
		this.pipeline = pipeline;
		this.instance = instance;
		this.applet = applet;
		this.datastore = database;
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
								if ( ! WorkStatus.isComplete( pipeline, iApplet, instance) ) {
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

	public static void updateStatus(RemusPipeline pipeline,
			RemusApplet remusApplet, RemusInstance inst, Map instanceInfo, RemusDB datastore) throws TException, NotImplemented {
		AppletRef ar = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, remusApplet.getID() + "/@instance" );		
		datastore.add(ar, 0, 0, inst.toString(), instanceInfo);
	}

	
	public long inputTimeStamp( ) {
		long out = 0;
		for ( String iRef : applet.getInputs() ) {
			if ( iRef.compareTo("?") != 0 ) {
				try {
					RemusApplet iApplet = pipeline.getApplet( iRef );
					if ( iApplet != null ) {
						long val = WorkStatus.getTimeStamp(pipeline, applet, instance);
						if ( out < val ) {
							out = val;
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


}
