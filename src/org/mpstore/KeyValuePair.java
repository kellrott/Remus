package org.mpstore;

public class KeyValuePair {
	private String instance;
	private long jobID;
	private String path;
	private long emitID;
	private Object key = null;
	private Object value = null;
	private MPStore datastore;
	
	KeyValuePair( MPStore datastore, String path, String instance, long jobID, long emitID ) {
		this.datastore = datastore;
		this.path = path;
		this.instance = instance;
		this.jobID = jobID;
		this.emitID = emitID;
	}
	
	KeyValuePair( MPStore datastore, String path, String instance, long jobID, long emitID, Object key, Object value ) {
		this.datastore = datastore;
		this.path = path;
		this.instance = instance;
		this.jobID = jobID;
		this.emitID = emitID;
		this.key = key;
		this.value = value;
	}


	public long getJobID() {		
		return jobID;
	}

	public long getEmitID() {
		return emitID;
	}

	public Object getKey() {
		if ( key == null )
			key = datastore.getKey( path, instance, jobID, emitID );
		return key;
	}

	public Object getValue() {
		if ( value == null )
			value = datastore.getValue( path, instance, jobID, emitID );
		return value;
	}
	
}
