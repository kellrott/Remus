package org.remusNet;

import java.util.Map;

import org.apache.thrift.TException;
import org.remusNet.thrift.AppletRef;
import org.remusNet.thrift.RemusDB.Iface;

public abstract class RemusDB implements Iface {

	abstract void init(Map params) throws ConnectionException;

	void add( AppletRef stack, long jobID, long emitID, String key, Object object ) throws TException {
		addData(stack, jobID,emitID, key, JSON.dumps(object));
	}
	
}
