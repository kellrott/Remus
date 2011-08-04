package org.remus.mapred;

import org.remus.serverNodes.BaseStackNode;

public interface MapperInterface {
	void initMapper(String config);
	public void map(BaseStackNode dataStack, MapCallback callback);
}
