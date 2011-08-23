package org.remus.core;

import java.util.List;

import org.remus.thrift.AppletRef;


public interface BaseStackNode {
	void add(AppletRef stack, long jobID, long emitID, String key, String data);
	boolean containsKey(AppletRef stack, String key);
	List<String> getValueJSON(AppletRef stack, String key);
	List<String> keySlice(AppletRef stack, String keyStart, int count);	
}
