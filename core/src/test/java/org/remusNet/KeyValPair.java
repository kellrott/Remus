package org.remusNet;

import org.remusNet.thrift.KeyValJSONPair;

public class KeyValPair extends KeyValJSONPair{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1468725096790017826L;

	public KeyValPair(KeyValJSONPair kv) {
		super(kv);
	}

	public KeyValPair(String key, Object value, long jobID, long emitID) {
		super(key, JSON.dumps(value), jobID, emitID);
	}
	
	public Object getValue() {
		return JSON.loads( getValueJson() );
	}
}
