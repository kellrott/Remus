package org.mpstore;

import java.io.InputStream;

public interface MPStore {
	
	public void init(Serializer serializer, String basePath);
	
	public void add(String file, String instance, long jobid, long order, String key, Object data);

	public Iterable<Object> get(String file, String instance, String key);
	
	public Iterable<String> listKeys(String file, String instance);
	
	public Iterable<KeyValuePair> listKeyPairs(String file, String instance);

	public boolean containsKey(String reqFile, String instance, String key);
	
	public void close();

	public void delete(String file, String instance);

	public void delete(String file, String instance, String key);

	public void writeAttachment(String file, String instance, String key, InputStream inputStream);

	public InputStream readAttachement(String string, String instance, String key); 

}
