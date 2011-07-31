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

	Object getValue() {
		return JSON.loads( getValueJson() );
	}
}
