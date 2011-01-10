package org.semweb.pluginterface;

import java.io.Serializable;


public interface WriterInterface extends InterfaceBase {

	void prepWriter(String config);
	Object write(Serializable val);

}
