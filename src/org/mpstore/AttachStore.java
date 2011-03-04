package org.mpstore;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface AttachStore {

	public void initAttachStore(Map paramMap);

	public void writeAttachment(String path, String instance, String key, String attachment, InputStream inputStream);

	public InputStream readAttachement(String path, String instance, String key, String attachment);

	List<String> listAttachment(String path, String instance, String key);

}
