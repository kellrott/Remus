package org.remus.core;

import java.util.List;

import org.remus.RemusDatabaseException;

public interface BaseStackNode {
	void add(String key, String data);
	boolean containsKey(String key);
	List<String> getValueJSON(String key);
	List<String> keySlice(String keyStart, int count);
	void delete(String key);	
}
