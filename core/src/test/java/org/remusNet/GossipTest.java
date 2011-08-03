package org.remusNet;

import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.remusNet.gossip.PeerServer;
import org.remusNet.thrift.BadPeerName;
import org.remusNet.thrift.PeerInfo;

public class GossipTest {

	public int startPort = 15068;
	
	PeerServer master;
	@Before public void setUp() throws TException, BadPeerName, InterruptedException, UnknownHostException, SocketException {
		PeerInfo info = new PeerInfo();
		info.master = true;
		info.name = "master";
		master = new PeerServer(info, null, null, startPort);
		Thread.sleep(1000);
	}
	
	
	@Test public void gossipTest() throws TException, BadPeerName, UnknownHostException, SocketException, InterruptedException {
		System.err.println( master.getAddress() );

		PeerInfo info1 = new PeerInfo();
		info1.database = true;
		info1.name = "db1";
		PeerServer dbPeer = new PeerServer(info1, "localhost", startPort, startPort + 1);		
		System.err.println(dbPeer.getPeers());
		
		PeerInfo info2 = new PeerInfo();
		info2.database = true;
		info2.name = "worker";
		PeerServer workPeer = new PeerServer(info2, "localhost", startPort, startPort + 2);		
		System.err.println(workPeer.getPeers());
		
		Thread.sleep(1000);

		dbPeer.close();	
		workPeer.close();
		System.err.println(master.getPeers());		
	}
	
	@After public void tearDown() throws TException {
		System.err.println("Tearing Down");
		master.close();
	}
	
}
