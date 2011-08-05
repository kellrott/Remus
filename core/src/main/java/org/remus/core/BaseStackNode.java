package org.remus.core;


public interface BaseStackNode {

	Iterable<String> getKeys();
	Iterable<Object> getData(String key);	
	
}
