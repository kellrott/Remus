package org.remus.mapred;

import java.io.Serializable;

public interface ReducerInterface extends InterfaceBase {

	void initReducer(String config);
	void reduce( Serializable key, Serializable val );
}
