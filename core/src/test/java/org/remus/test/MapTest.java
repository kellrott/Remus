package org.remus.test;


import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.remus.KeyValPair;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.RemusManager;
import org.remus.core.RemusApp;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class MapTest {
	PluginManager pm;

	@Before public void setup() throws Exception {

		Map initMap = new HashMap();

		Map configMap = new HashMap();
		configMap.put("columnFamily", "remusTable");
		configMap.put("keySpace", "remus");
		configMap.put("instColumns", "false");
		Map dbMap = new HashMap();
		dbMap.put("config", configMap);
		initMap.put("org.remus.cassandra.Server", dbMap);

		Map manMap = new HashMap();
		initMap.put("org.remus.manage.WorkManager", manMap);
		
		Map jsMap = new HashMap();
		initMap.put("org.remus.js.JSWorker", jsMap);
		pm = new PluginManager(initMap);
		pm.start();

	}

	@Test public void mapTest() throws RemusDatabaseException, TException, NotImplemented {

		RemusApp app = new RemusApp(pm.getDataServer(), pm.getAttachStore());
		InputStream is = MapTest.class.getResourceAsStream("jsPipeline.json");
		Object pipelineDesc = JSONValue.parse(new InputStreamReader(is));

		app.putPipeline("testPipeline", pipelineDesc);
		RemusPipeline pipe = app.getPipeline("testPipeline");
		System.err.println(pipe.getMembers());

		Map subMap = (Map)JSONValue.parse("{ \"_applets\" : [\"testMap\", \"inputStack\"]  }");
		
		RemusInstance inst = pipe.handleSubmission("testSubmit", subMap);

		AppletRef ar = new AppletRef("testPipeline", inst.toString(), "inputStack");

		RemusDB dataServer = pm.getDataServer();
		dataServer.add(ar, 0, 0, "a", "four");
		dataServer.add(ar, 0, 0, "b", "score");
		dataServer.add(ar, 0, 0, "c", "and");
		dataServer.add(ar, 0, 0, "d", "seven");
		dataServer.add(ar, 0, 0, "e", "years");
		dataServer.add(ar, 0, 0, "f", "ago");
		
		RemusManager manage = pm.getManager();
		manage.scheduleRequest();
		//app.setupPipeline( )

		
		AppletRef dr = new AppletRef("testPipeline", inst.toString(), "testMap");
		for ( KeyValPair kv : dataServer.listKeyPairs(dr) ) {
			Map data = (Map)kv.getValue();
			Assert.assertTrue( data.get("word").toString().length() == (Long)data.get("len") );
			System.err.println(kv.getKey() + " " + kv.getValue() );
		}
		

	}

	@After public void tearDown() throws RemusDatabaseException, TException {
		
		RemusApp app = new RemusApp(pm.getDataServer(), pm.getAttachStore());
		RemusPipeline pipe = app.getPipeline("testPipeline");
		app.deletePipeline(pipe);
		
		pm.close();
	}

}
