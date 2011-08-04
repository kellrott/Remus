package org.remus.core;

import org.remus.RemusDB;

public interface DBObject {

	void load(RemusDB db);
	void store(Object desc, RemusDB db);
	
}
