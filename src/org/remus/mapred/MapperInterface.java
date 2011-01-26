package org.remus.mapred;

import java.io.Serializable;


public interface MapperInterface extends InterfaceBase {

	void prepMapper(String config);
	void map( Serializable val, MapCallback callback );
}
