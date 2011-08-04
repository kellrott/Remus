package org.remus.core;

import org.remus.RemusAttach;
import org.remus.RemusDB;

public interface DBObject {

	void load(RemusDB db, RemusAttach attach);
	
}
