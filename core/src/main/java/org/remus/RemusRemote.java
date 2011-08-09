package org.remus;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.remus.thrift.RemusNet;

public class RemusRemote {

	public static RemusNet.Iface getClient(String host, int port) throws TTransportException {
		TSocket transport = new TSocket(host, port);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		transport.open();
		RemusNet.Client client = new RemusNet.Client(protocol);
		return client;
	}

}
