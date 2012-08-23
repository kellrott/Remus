package org.remus;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONAware;
import org.remus.thrift.KeyValJSONPair;

public class KeyValPair extends KeyValJSONPair implements JSONAware {

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

	@Override
	public String toJSONString() {
		Map out = new HashMap();
		out.put(key, getValue());
		return JSON.dumps(out);
	}
}
