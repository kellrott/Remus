package org.remus;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
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

	}

	@Override
	public void addPeer(PeerInfoThrift info) throws BadPeerName, TException, NotImplemented {
		if (!silent) {
			TSocket transport = new TSocket(server, port);
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			transport.open();
			RemusNet.Client client = new RemusNet.Client(protocol);
			client.addPeer(info);
			transport.close();
		}
	}


	@Override
	public void delPeer(String peerName) throws TException, NotImplemented {
		if (!silent) {
			TSocket transport = new TSocket(server, port);
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			transport.open();
			transport.open();
			RemusNet.Client client = new RemusNet.Client(protocol);
			client.delPeer(peerName);
			transport.close();	
		}
	}



	@Override
	public List<PeerInfoThrift> getPeers() throws TException, NotImplemented {
		logger.info("Getting peers from " + server + ":" + port);
		TSocket transport = new TSocket(server, port);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		transport.open();
		RemusNet.Client client = new RemusNet.Client(protocol);
		List<PeerInfoThrift> out = client.getPeers();
		transport.close();
		return out;
	}

}
