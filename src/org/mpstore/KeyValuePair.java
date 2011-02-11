package org.mpstore;

public class KeyValuePair {
	private long jobID;
	private long emitID;
	private String key = null;
	private Object value = null;
	
	public KeyValuePair( long jobID, long emitID, String key, Object value ) {
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
