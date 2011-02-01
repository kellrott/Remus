package org.mpstore;

import java.io.File;

public interface MPStore {
	
	abstract public void init(Serializer serializer, String basePath);
	
	abstract public void add(File file, String instance, long jobid, long order, Object key, Object data);

	abstract public Iterable<Object> get(File file, String instance, Object key);
	
	abstract public Iterable<Object> listKeys(File file, String instance);
	
	abstract public Iterable<KeyValuePair> listKeyPairs(File file, String instance);

	abstract public boolean containsKey(File reqFile, String instance, Object string);

	abstract public KeyValuePair get(File reqFile, String instance, long jobID, long emitID );

	abstract public Object getKey(String path, String instance, long jobID, long emitID);
	abstract public Object getValue(String path, String instance, long jobID, long emitID);
	
}
