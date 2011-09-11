package org.remus.plugin;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Map;

import org.remus.thrift.RemusNet;

import org.remus.PeerInfo;

public interface PluginInterface extends RemusNet.Iface {

	void init(Map params) throws Exception;
	PeerInfo getPeerInfo();
	void setupPeer(PeerManager pm) throws UnknownHostException, SocketException;
	void start(PluginManager pluginManager) throws Exception;
	void stop();
}
