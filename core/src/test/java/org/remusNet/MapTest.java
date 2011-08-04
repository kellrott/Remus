package org.remusNet;


import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.remus.RemusDB;
import org.remus.RemusManager;
import org.remus.core.RemusApp;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PluginManager;
import org.remus.server.RemusDatabaseException;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class MapTest {
	PluginManager pm;

	@Before public void setup() throws Exception {

		Map initMap = new HashMap();

		Map dbMap = new HashMap();
		dbMap.put("org.mpstore.ThriftStore.columnFamily", "remusTable");
		dbMap.put("org.mpstore.ThriftStore.keySpace", "remus");
		dbMap.put("org.mpstore.ThriftStore.instColumns", "false");
		initMap.put("org.remus.cassandra.Server", dbMap);

		Map manMap = new HashMap();
		initMap.put("org.remus.manage.WorkManager", manMap);

		pm = new PluginManager(initMap);
		pm.start();

	}

	@Test public void mapTest() throws RemusDatabaseException, TException, NotImplemented {

		RemusApp app = new RemusApp(pm);
		InputStream is = MapTest.class.getResourceAsStream("jsPipeline.json");
		Object pipelineDesc = JSONValue.parse(new InputStreamReader(is));

		app.putPipeline("testPipeline", pipelineDesc);
		RemusPipeline pipe = app.getPipeline("testPipeline");
		System.err.println(pipe.getMembers());

		Map subMap = (Map)JSONValue.parse("{ \"_applets\" : [\"testMap\", \"inputStack\"]  }");
		
		RemusInstance inst = pipe.handleSubmission("testSubmit", subMap);

		AppletRef ar = new AppletRef("testPipeline", inst.toString(), "inputStack");

		RemusDB dataServer = pm.getDataServer();
		dataServer.add(ar, 0, 0, "hello", "world");
		
		RemusManager manage = pm.getManager();
		manage.scheduleRequest();
		//app.setupPipeline( )


	}

	@After public void tearDown() {
		pm.close();
	}

}
