package org.semweb.pluginterface;

import java.io.Serializable;

public interface ReducerInterface {

	void prepReducer(String config);
	void reduce( Serializable key, Serializable val );
}
