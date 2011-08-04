package org.remus.mapred;

import java.io.Serializable;


public interface PipeInterface {

	void initWriter(String config);
	Object write(Serializable val);

}
