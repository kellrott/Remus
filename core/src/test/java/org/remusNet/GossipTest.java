package org.remusNet;

import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.remus.gossip.PeerServer;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.PeerType;
import org.remus.PeerInfo;

public class GossipTest {

	public int startPort = 15068;

	PeerServer master;
	@Before public void setUp() throws TException, BadPeerName, InterruptedException, UnknownHostException, SocketException {
		PeerInfo info = new PeerInfo();
		info.peerType = PeerType.MANAGER;
		info.name = "master";
		master = new PeerServer(info, null, null, startPort);
		Thread.sleep(1000);
	}

/*
	@Test public void gossipTest() throws TException, BadPeerName, UnknownHostException, SocketException, InterruptedException {

		PeerInfo info1 = new PeerInfo();
		info1.database = true;
		info1.name = "db1";
		PeerServer dbPeer = new PeerServer(info1, "localhost", startPort, startPort + 1);		
		System.err.println(dbPeer.getPeers());

		PeerInfo info2 = new PeerInfo();
		info2.database = true;
		info2.name = "worker";
		PeerServer workPeer = new PeerServer(info2, "localhost", startPort, startPort + 2);		

		master.doPing();

		System.err.println(workPeer.getPeers());

		Thread.sleep(1000);

		dbPeer.close();	
		workPeer.close();
		System.err.println(master.getPeers());		
	}
 */
	
	private static int GROUP_SIZE = 30;
	@Test public void largeTest() throws TException, BadPeerName, UnknownHostException, SocketException, InterruptedException {

		PeerServer [] peers = new PeerServer[GROUP_SIZE];

		int allocCount=0;
		for ( int i = 0; i < GROUP_SIZE; i++ ) {
			PeerInfo info1 = new PeerInfo();
			info1.peerType = PeerType.DB_SERVER;
			info1.name = "db_" + i;
			try {
				PeerServer dbPeer = new PeerServer(info1, "localhost", startPort, startPort + i);		
				peers[i] = dbPeer;
				allocCount++;
			} catch (TException e) {
				peers[i] = null;
			}
		}

		master.doPing();
		System.err.println( "SERVER_COUNT=" + master.getPeers().size() + "/" + allocCount );
		Thread.sleep(20000);
		System.err.println("PING");
		master.doPing();
		Thread.sleep(10000);
		System.err.println( "SERVER_COUNT=" + master.getPeers().size() + "/" + allocCount );
		
		for ( int i = 0; i < GROUP_SIZE; i++ ) {
			if ( peers[i] != null ) {
				peers[i].close();
			}
		}
		
		System.err.println( "SERVER_COUNT=" + master.getPeers().size() + "/" + allocCount );
	}

	@After public void tearDown() throws TException {
		System.err.println( "FINAL SERVER_COUNT=" + master.getPeers().size() );
		master.close();
	}

}
