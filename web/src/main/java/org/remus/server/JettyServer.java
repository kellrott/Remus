package org.remus.server;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.remus.JSON;
import org.remus.PeerInfo;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.RemusWeb;
import org.remus.core.BaseStackNode;
import org.remus.mapred.MapReduceCallback;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.JobState;
import org.remus.thrift.JobStatus;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerType;
import org.remus.thrift.RemusNet;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.WorkMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JettyServer extends RemusWeb {

	Map params;
	Server server;

	public static final int DEFAULT_PORT = 16016;

	private Logger logger;
	private PluginManager pm;

	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.name = "Jetty Remus Web Server";
		out.peerType = PeerType.WEB_SERVER;
		return out;
	}

	Map<AppletRef, BaseStackNode> stackMap;

	@Override
	public void init(Map params) {
		this.params = params;		
		logger = LoggerFactory.getLogger(JettyServer.class);
		stackMap = new HashMap<AppletRef, BaseStackNode>();
	}	

	@Override
	public void start(PluginManager pluginManager) throws RemusDatabaseException {
		this.pm = pluginManager;
		System.setProperty("org.mortbay.http.HttpRequest.maxFormContentSize", "0");		

		int serverPort = DEFAULT_PORT;
		if (params != null && params.containsKey("org.remus.port")) {
			serverPort = Integer.parseInt(params.get("org.remus.port").toString());
		}

		server = new Server(serverPort);
		Context root = new Context(server, "/", Context.SESSIONS);
		ServletHolder sh = new ServletHolder(new MasterServlet(this));

		/*
		for (Map.Entry<Object, Object> propItem : prop.entrySet()) {
			String key = (String) propItem.getKey();
			String value = (String) propItem.getValue();
			sh.setInitParameter(key, value);
		}
		 */		

		root.addServlet(sh, "/*");

		Thread serverThread = new Thread() {	
			@Override
			public void run() {
				try {
					server.start();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
			}		
		};		
		serverThread.start();		
	}

	@Override
	public void stop() {
		try {
			server.stop();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public RemusAttach getAttachStore() {		
		try {
			return (RemusAttach) pm.getPeer(pm.getAttachStore());
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public RemusDB getDataStore() {
		try {
			return (RemusDB) pm.getPeer(pm.getDataServer());
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}


	@Override
	public void jsRequest(String string, WorkMode mode,
			BaseStackNode appletView, MapReduceCallback mapReduceCallback) {
		String peerID = pm.getPeerID(this);
		logger.info("Javascript Query: " + peerID);
		RemusNet.Iface jsWorker = null;
		//RemusManager manager = pm.getManager();
		//jsWorker = manager.getWorker("javascript");

		try {
			for (String worker : pm.getWorkers("javascript")) {
				jsWorker = pm.getPeer(worker);
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		WorkDesc work = new WorkDesc();
		work.mode = mode;
		work.infoJSON = JSON.dumps(new HashMap());
		work.workStart = 0;
		work.workEnd = 1;
		work.lang = "javascript";

		try {
			String jobID = jsWorker.jobRequest(peerID, null, work);			
			boolean done = false;
			do {
				JobStatus stat = jsWorker.jobStatus(jobID);
				if (stat.status == JobState.QUEUED || stat.status == JobState.WORKING) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					done = true;
				}
			} while (!done);
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	@Override
	public String status() throws TException {
		return "OK";
	}

	@Override
	public List<String> getValueJSON(AppletRef stack, String key)
	throws NotImplemented, TException {
		logger.info("WEB_DB GET: " + stack + " " + key);
		return Arrays.asList("test");
	}

}
