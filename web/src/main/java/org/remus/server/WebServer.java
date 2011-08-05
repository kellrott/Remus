package org.remus.server;

import java.util.Map;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.remus.PeerInfo;
import org.remus.core.WebAgent;
import org.remus.plugin.PluginManager;
import org.remus.thrift.PeerType;

public class WebServer implements WebAgent {

	Map params;
	Server server;

	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.peerType = PeerType.WEB_SERVER;
		return out;
	}

	@Override
	public void init(Map params) {
		this.params = params;		
	}	

	@Override
	public void start(PluginManager pluginManager) throws RemusDatabaseException {
		System.setProperty("org.mortbay.http.HttpRequest.maxFormContentSize", "0");		

		int serverPort = 16016;
		if ( params.containsKey("org.remus.port") ) {
			serverPort = Integer.parseInt(params.get("org.remus.port").toString());
		}

		server = new Server(serverPort);
		Context root = new Context(server, "/", Context.SESSIONS);
		ServletHolder sh = new ServletHolder(new MasterServlet(pluginManager));


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

}
