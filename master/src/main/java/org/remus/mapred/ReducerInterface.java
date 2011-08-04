package org.remus.mapred;

import java.io.Serializable;

public interface ReducerInterface  {

	void initReducer(String config);
	void reduce( Serializable key, Serializable val );
}
