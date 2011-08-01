package org.remusNet.cassandra;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.SoftReferenceObjectPool;

public class ThriftClientPool {
	private ObjectPool clientPool;
	public ThriftClientPool( String serverName, int serverPort, String keySpace ) {
		clientPool = new SoftReferenceObjectPool( new ThriftClientFactory(serverName,serverPort,keySpace) );
	}
	public Client borrowObject() throws Exception {
		return (Client)clientPool.borrowObject();
	}
	public void invalidateObject(Client client) throws Exception {
		clientPool.invalidateObject(client);
	}
	public void returnObject(Client client) throws Exception {
		clientPool.returnObject(client);
	}
	
}
