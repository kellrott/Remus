package org.remus;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.thrift.TException;
import org.remus.plugin.PeerManager;
import org.remus.plugin.PluginInterface;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.RemusNet;

abstract public class RemusPeer implements RemusNet.Iface, PluginInterface {

	PeerManager peerManager;

	@Override
	public void setupPeer(PeerManager pm) throws UnknownHostException, SocketException {
		this.peerManager = pm;
	}
	
	@Override
	public List<PeerInfoThrift> peerInfo(List<PeerInfoThrift> info)
			throws NotImplemented, BadPeerName, TException {
		return peerManager.peerInfo(info);
	}
}
