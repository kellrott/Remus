package org.mpstore;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface AttachStore {

	public void initAttachStore(Map paramMap);

	public void writeAttachment(String path, String instance, String key, String attachment, InputStream inputStream);

	public InputStream readAttachement(String path, String instance, String key, String attachment);

	public List<String> listAttachment(String path, String instance, String key);
	public Iterable<String> listKeys(String path, String instance);

	public void delete(String path, String instance);

	public boolean hasAttachment(String path, String instance, String key, String name);

	public boolean hasKey(String path, String instance, String key);

	public Map<String,String> getConfig();

}
