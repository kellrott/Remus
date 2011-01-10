package org.semweb.pluginterface;

import java.io.Serializable;


public interface MapperInterface extends InterfaceBase {

	void prepMapper(String config);
	void map( Serializable val, MapCallback callback );
}
