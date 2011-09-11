package org.remus.test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;


import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.remus.plugin.PluginManager;

public class NameTest {

	PluginManager pm1, pm2;

	@Before public void setup() throws Exception {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.DEBUG);
	}

	private static int PEER_COUNT = 100;
	@Test public void peerTest() throws Exception {

		PluginManager [] pms = new PluginManager[PEER_COUNT];

		Map initMap = new HashMap();		
		Map jsMap = new HashMap();
		List l = new LinkedList();
		l.add("localhost:18000");
		initMap.put("seeds", l);

		Map jsServerMap = new HashMap();
		jsServerMap.put("port", 18000);
		jsMap.put("server", jsServerMap);

		initMap.put("org.remus.js.JSWorker", jsMap);

		for (int i = 0;  i < PEER_COUNT; i++) {
			jsServerMap.put("port", 18000+i);
			pms[i] = new PluginManager(initMap);
			pms[i].start();
		}

		int tries = 0;
		boolean done = true;
		do {
			for (int i = 0; i < PEER_COUNT; i++) {
				pms[i].getPeerManager().update();
			}
			done = true;
			for (int i = 0; i < PEER_COUNT; i++) {
				if ( pms[i].getPeerManager().getPeers().size() != PEER_COUNT ) {
					done = false;
				}
			}
			tries++;
		} while (tries < 20 && !done);

		System.err.println("Completion took " + tries + " tries");

		Set<Integer> closeSet = new HashSet<Integer>();
		for (int i=0; i < 10; i++) {
			Random r = new Random();
			int c = r.nextInt(PEER_COUNT);
			if ( !closeSet.contains(c)) {
				pms[c].close();
				closeSet.add(c);
			}
		}
		
		tries = 0;
		do {
			for (int i = 0; i < PEER_COUNT; i++) {
				pms[i].getPeerManager().update();
			}

			done = true;
			for (int i = 0; i < PEER_COUNT; i++) {
				if (!closeSet.contains(i)) {
					if( pms[i].getPeerManager().getPeers().size() != PEER_COUNT - closeSet.size()) {
						done = false;
					}
				}
			}
			tries++;
		} while (tries < 20 && !done);
		System.err.println("Adjustment took " + tries + " tries");

		for (int i = 0;  i < PEER_COUNT; i++) {
			pms[i].close();			
		}

	}

	@After public void tearDown() {
	}

}
