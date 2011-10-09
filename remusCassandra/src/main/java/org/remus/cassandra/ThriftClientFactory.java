package org.remus.cassandra;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
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
	ArrayList<String> hostList;
	int curHost = 0;

	public ThriftClientFactory(String serverName, int serverPort, String keySpace) throws Exception {
		this.serverName = serverName;
		this.serverPort = serverPort;
		this.keySpace = keySpace;

		TTransport tr = new TSocket(serverName, serverPort);
		TFramedTransport tf = new TFramedTransport(tr);	 
		TProtocol proto = new TBinaryProtocol(tf);	 
		tf.open();
		Client client = new Client(proto);	
		Set<String> tList = new HashSet<String>();
		try {
			for (TokenRange tokr : client.describe_ring(keySpace)) {
				for (String host : tokr.getEndpoints()) {
					tList.add(host);
				}
			}
		} catch (InvalidRequestException e) {
			tList.add(serverName);
		}
		hostList = new ArrayList<String>(tList);
	}

	@Override
	public Object makeObject() throws Exception {
		TTransport tr = new TSocket(hostList.get(curHost), serverPort);
		curHost = (curHost + 1) % hostList.size();

		TFramedTransport tf = new TFramedTransport(tr);	 
		TProtocol proto = new TBinaryProtocol(tf);	 
		tf.open();
		Client client = new Client(proto);	
		client.set_keyspace(keySpace);
		return client;
	}

	@Override
	public void destroyObject(Object obj) throws Exception {
		((Client) obj).getInputProtocol().getTransport().close();
	}
	@Override
	public boolean validateObject(Object obj) {
		return ((Client) obj).getInputProtocol().getTransport().isOpen();
	}
}

