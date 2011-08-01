package org.remus.mapred;


public interface MapCallback {
	void emit(String key, Object val);
}
