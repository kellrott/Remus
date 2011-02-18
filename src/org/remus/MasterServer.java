package org.remus;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class MasterServer {

	public static void main(String args[]) {
		try {
			System.setProperty("org.mortbay.http.HttpRequest.maxFormContentSize", "0");

			Properties prop = new Properties();
			prop.load( new FileInputStream( new File( args[0] ) ) );

			int serverPort = 16016;
			if ( prop.containsKey("org.remus.port") ) {
				serverPort = Integer.parseInt( prop.getProperty("org.remus.port") );
			}
			Server server = new Server(serverPort);
			Context root = new Context(server,"/",Context.SESSIONS);
			ServletHolder sh = new ServletHolder(new MasterServlet());

			for (Map.Entry<Object, Object> propItem : prop.entrySet()) {
				String key = (String) propItem.getKey();
				String value = (String) propItem.getValue();
				sh.setInitParameter(key, value);
			}

			root.addServlet(sh, "/*");
			server.start();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
