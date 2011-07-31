package org.remusNet;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


class ThriftClientFactory extends BasePoolableObjectFactory {

	String serverName, keySpace;
	int serverPort;
	
	public ThriftClientFactory(String serverName, int serverPort, String keySpace) {
		this.serverName = serverName;
		this.serverPort = serverPort;
		this.keySpace = keySpace;
	}
	
	@Override
	public Object makeObject() throws Exception {
		TTransport tr = new TSocket(serverName, serverPort);
		TFramedTransport tf = new TFramedTransport(tr);	 
		TProtocol proto = new TBinaryProtocol(tf);	 
		tf.open();
		Client client = new Client(proto);	
		client.set_keyspace(keySpace);
		return client;
	}

	@Override
	public void destroyObject(Object obj) throws Exception {
		((Client)obj).getInputProtocol().getTransport().close();
	}
	@Override
	public boolean validateObject(Object obj) {
		return ((Client)obj).getInputProtocol().getTransport().isOpen();
	}
}

