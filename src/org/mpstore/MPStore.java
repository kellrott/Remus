package org.mpstore;

import java.util.List;
import java.util.Map;

public interface MPStore {
	
	public void initMPStore(Serializer serializer, Map paramMap);
	
	public void add(String path, String instance, long jobid, long emitID, String key, Object data);
	public void add(String path, String instance, List<KeyValuePair> inputList); 

	public Iterable<Object> get(String path, String instance, String key);
	
	public Iterable<String> listKeys(String path, String instance);
	
	public Iterable<KeyValuePair> listKeyPairs(String oath, String instance);

	public boolean containsKey(String path, String instance, String key);

	public long keyCount( String path, String instance, int maxCount );
	
	public Iterable<String> keySlice( String path, String instance, String startKey, int count );

	public void delete(String path, String instance);
	
	public void delete(String path, String instance, String key);
	
	//public void delete(String path, String instance, String key, long jobID, long emitID);

	public long getTimeStamp(String path, String instance);

	public void close();

}
