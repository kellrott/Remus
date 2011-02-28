package org.mpstore;

import java.io.InputStream;
import java.util.List;

public interface MPStore {
	
	public void init(Serializer serializer, String basePath);
	
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
	
	public void writeAttachment(String path, String instance, String key, InputStream inputStream);

	public InputStream readAttachement(String path, String instance, String key);

	public void close();

}
