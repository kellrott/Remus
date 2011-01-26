package org.remus.mapred;

import java.io.Serializable;


public interface PipeInterface extends InterfaceBase {

	void prepWriter(String config);
	Object write(Serializable val);

}
