package org.remus.mapred;

import java.io.Serializable;

public interface MergerInterface extends InterfaceBase {
	void initMerger(String config);
	void merge( Serializable left_key, Serializable left_vals, Serializable right_key, Serializable right_vals );
}
