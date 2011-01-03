package org.semweb.plugins;

import org.semweb.config.ConfigMap;
import org.semweb.config.RequestKey;

public abstract class DataRequest {

	public abstract RequestKey request(ConfigMap config);
	
	
}
