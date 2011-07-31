package org.remusNet;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remusNet.thrift.AppletRef;
import org.remusNet.thrift.KeyValJSONPair;
import org.remusNet.thrift.RemusDBThrift.Iface;

public abstract class RemusDB implements Iface {

	abstract void init(Map params) throws ConnectionException;

	void add( AppletRef stack, long jobID, long emitID, String key, Object object ) throws TException {
		addData(stack, jobID,emitID, key, JSON.dumps(object));
	}
	
	
	public List<Object> get(AppletRef stack, String key)
			throws TException {
		
		List<String> i = getValueJSON(stack, key);

		List<Object> out = new ArrayList<Object>(i.size());
		for ( String j : i ) {
			out.add(JSON.loads(j));
		}
		return out;
	}
	
	public List<KeyValPair> keyValSlice(AppletRef stack,
			String startKey, int count) throws TException {
		List<KeyValJSONPair> i = keyValJSONSlice(stack, startKey, count);
		
		List<KeyValPair> out = new ArrayList<KeyValPair>( i.size() );
		for ( KeyValJSONPair kv : i ) {
			out.add( new KeyValPair(kv) );
		}
		return out;
	}
}
