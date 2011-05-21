package org.mpstore;

import java.util.List;
import java.util.Map;

/**
 * Map Store: Interface for storage system that is designed to take 
 * key/value pairs and store them in groups. By using the jobID/emitID combo
 * multiple values can be place into the same key. This is so that later, values 
 * can be retrieved in a set, by common key. Keys are strings, while values are
 * objects that can be serialized by the Serializer (right now, commonly 
 * org.json.simple based serializer)
 * 
 * @author kellrott@gmail.com
 *
 */

public interface MPStore {
	
	public void initMPStore(Serializer serializer, Map paramMap) throws MPStoreConnectException;
	
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
