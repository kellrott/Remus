package org.semweb.template;

import java.io.InputStream;
import java.util.Map;

public interface TemplateInterface {
	public void init();
	public String replaceCode(String id);
	public InputStream render(String text, Map<String, String> inMap);
}
