package org.mpstore;

public interface MPStore {
	
	abstract public void init(Serializer serializer, String basePath);
	
	abstract public void add(String file, String instance, long jobid, long order, Object key, Object data);

	abstract public Iterable<Object> get(String file, String instance, Object key);
	
	abstract public Iterable<Object> listKeys(String file, String instance);
	
	abstract public Iterable<KeyValuePair> listKeyPairs(String file, String instance);

	abstract public boolean containsKey(String reqFile, String instance, Object key);
	
	
}
