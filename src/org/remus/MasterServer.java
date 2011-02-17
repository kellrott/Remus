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

			Server server = new Server(16016);
			Context root = new Context(server,"/",Context.SESSIONS);
			ServletHolder sh = new ServletHolder(new MasterServlet());

			for (Map.Entry<Object, Object> propItem : prop.entrySet()) {
				String key = (String) propItem.getKey();
				String value = (String) propItem.getValue();
				sh.setInitParameter(key, value);
			}

			//sh.setInitParameter("org.remus.mpstore", "org.mpstore.SQLStore");
			//sh.setInitParameter("org.remus.mpstore", "org.mpstore.ThriftStore");			
			//sh.setInitParameter("org.remus.srcdir", args[0] );
			//sh.setInitParameter("org.remus.workdir", args[1] );
			root.addServlet(sh, "/*");
			server.start();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
