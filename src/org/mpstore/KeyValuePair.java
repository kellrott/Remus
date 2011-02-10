package org.mpstore;

public class KeyValuePair {
	private String instance;
	private long jobID;
	private String path;
	private long emitID;
	private String key = null;
	private Object value = null;
	
	KeyValuePair( String path, String instance, long jobID, long emitID, String key, Object value ) {
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

	public String getKey() {		
		return key;
	}

	public Object getValue() {
		return value;
	}
	
}
