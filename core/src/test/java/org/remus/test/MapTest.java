package org.remus.test;


import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.RemusManager;
import org.remus.core.PipelineDesc;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusApp;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PeerManager;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;

public class MapTest {
	PluginManager plugManager;
	PeerManager pm;
	@Before public void setup() throws Exception {

		InputStream is = MapTest.class.getResourceAsStream("config.json");
		
		Object initMap = JSONValue.parse(new InputStreamReader(is));
		
		plugManager = new PluginManager((Map) initMap);
		plugManager.start();
		pm = plugManager.getPeerManager();
	}

	@Test public void mapTest() throws RemusDatabaseException, TException, NotImplemented, InterruptedException {

		RemusApp app = new RemusApp((RemusDB) pm.getPeer(pm.getDataServer()), (RemusAttach) pm.getPeer(pm.getAttachStore()));
		
		RemusPipeline pipe = app.getPipeline("testPipeline");
		if (pipe != null) {
			app.deletePipeline(pipe);
		}
		InputStream is = MapTest.class.getResourceAsStream("jsPipeline.json");
		Object pipelineDesc = JSONValue.parse(new InputStreamReader(is));

		app.putPipeline("testPipeline", new PipelineDesc(pipelineDesc));
		pipe = app.getPipeline("testPipeline");
		System.err.println(pipe.getMembers());

		Map subMap = (Map) JSONValue.parse("{ \"_applets\" : [\"testMap\", \"inputStack\"]  }");
		
		RemusInstance inst = pipe.handleSubmission("testSubmit", new PipelineSubmission(subMap));

		AppletRef ar = new AppletRef("testPipeline", inst.toString(), "inputStack");

		RemusDB dataServer = (RemusDB) pm.getPeer(pm.getDataServer());
		dataServer.add(ar, 0, 0, "a", "four");
		dataServer.add(ar, 0, 0, "b", "score");
		dataServer.add(ar, 0, 0, "c", "and");
		dataServer.add(ar, 0, 0, "d", "seven");
		dataServer.add(ar, 0, 0, "e", "years");
		dataServer.add(ar, 0, 0, "f", "ago");
		
		RemusNet.Iface manage = pm.getPeer(pm.getManager());

		boolean done = false;
		do {
			manage.scheduleRequest();		
			Thread.sleep(10000);
			Map info = (Map) JSON.loads(manage.scheduleInfoJSON());
			if (((String) info.get("activeCount")).compareTo("0") == 0) {
				done = true;
			}
			System.out.println(info);
		} while (!done);
		AppletRef dr = new AppletRef("testPipeline", inst.toString(), "testMap");
		for (KeyValPair kv : dataServer.listKeyPairs(dr)) {
			Map data = (Map) kv.getValue();
			Assert.assertTrue(data.get("word").toString().length() == (Long) data.get("len"));
			System.err.println(kv.getKey() + " " + kv.getValue());
		}
		

	}

	@After public void tearDown() throws RemusDatabaseException, TException {
		
		RemusApp app = new RemusApp((RemusDB) pm.getPeer(pm.getDataServer()), (RemusAttach) pm.getPeer(pm.getAttachStore()));
		RemusPipeline pipe = app.getPipeline("testPipeline");
		app.deletePipeline(pipe);
		
		plugManager.close();
	}

}
