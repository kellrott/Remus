package org.remus.serverNodes;

import java.util.Iterator;

public interface BaseStackNode {

	Iterable<String> getKeys();
	Iterable<Object> getData(String key);	
	
}
