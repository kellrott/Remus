package org.remus.mapred;

import java.io.Serializable;


public interface MapperInterface extends InterfaceBase {

	void initMapper(String config);
	void map( Serializable val, MapCallback callback );
}
