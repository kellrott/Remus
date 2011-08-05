package org.remus.plugin;

import java.util.Map;

import org.remus.PeerInfo;

public interface PluginInterface {

	void init(Map params) throws Exception;
	PeerInfo getPeerInfo();
	void start(PluginManager pluginManager) throws Exception;
	void stop();
}
