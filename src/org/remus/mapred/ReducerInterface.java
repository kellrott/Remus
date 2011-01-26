package org.remus.mapred;

import java.io.Serializable;

public interface ReducerInterface extends InterfaceBase {

	void prepReducer(String config);
	void reduce( Serializable key, Serializable val );
}
