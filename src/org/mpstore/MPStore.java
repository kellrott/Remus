package org.mpstore;

public interface MPStore {
	
	public void init(Serializer serializer, String basePath);
	
	public void add(String file, String instance, long jobid, long order, Object key, Object data);

	public Iterable<Object> get(String file, String instance, Object key);
	
	public Iterable<Object> listKeys(String file, String instance);
	
	public Iterable<KeyValuePair> listKeyPairs(String file, String instance);

	public boolean containsKey(String reqFile, String instance, Object key);
	
	public void close(); 
}
