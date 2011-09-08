package org.remus;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.remus.plugin.PluginInterface;
import org.remus.plugin.PluginManager;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.PeerType;
import org.remus.thrift.RemusNet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemusIDClient extends RemusIDServer {

	private Logger logger;
	String server;
	int port;
	private PluginManager plugManager;
	private boolean silent;

	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo p = new PeerInfo();
		p.name = "Client ID Server";
		p.peerType = PeerType.NAME_SERVER;
		return p;
	}

	@Override
	public void init(Map params) throws Exception {
		logger = LoggerFactory.getLogger(RemusIDClient.class);
		silent = false;
		if (params.containsKey("silent")){
			silent = (Boolean) params.get("silent");
		}
		server = (String) params.get("host");
		port =  Integer.parseInt(params.get("port").toString());
	}

	@Override
	public void preStart(PluginManager pm) throws Exception {
		this.plugManager = pm;
		for (PluginInterface pi : pm.getPlugins()) {
			PeerInfo info = pi.getPeerInfo();
			info.setPeerID(UUID.randomUUID().toString());
			info.setHost(getDefaultAddress());
			info.setPort(pm.addLocalPeer(info.peerID, (RemusNet.Iface) pi));
			addPeer(info);
		}		
	}

	@Override	
	public void start(PluginManager pluginManager) throws Exception {

	}

	@Override
	public void stop() {
		synchronized (clientLock) {
			if (client != null) {
				client.getInputProtocol().getTransport().close();
				client = null;
			}
		}
	}

	Byte clientLock = new Byte((byte)0);
	RemusNet.Client client = null;

	private void openClient() throws TTransportException {
		if (client == null || !client.getInputProtocol().getTransport().isOpen()) {
			TSocket transport = new TSocket(server, port);
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			transport.open();
			client = new RemusNet.Client(protocol);
		}
	}

	@Override
	public void addPeer(PeerInfoThrift info) throws BadPeerName, TException, NotImplemented {
		if (!silent) {
			synchronized (clientLock) {
				openClient();
				client.addPeer(info);
			}
		}
	}


	@Override
	public void delPeer(String peerName) throws TException, NotImplemented {
		if (!silent) {
			synchronized (clientLock) {
				openClient();
				client.delPeer(peerName);
			}	
		}
	}


	@Override
	public List<PeerInfoThrift> getPeers() throws TException, NotImplemented {
		logger.debug("Getting peers from " + server + ":" + port);
		synchronized (clientLock) {
			openClient();
			List<PeerInfoThrift> out = client.getPeers();
			return out;
		}
	}


	@Override
	public String status() throws TException {
		return "OK";
	}
}
