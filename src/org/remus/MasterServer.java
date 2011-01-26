package org.remus;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class MasterServer {
	
	public static void main(String args[]) {
		try {
			Server server = new Server(16016);
			Context root = new Context(server,"/",Context.SESSIONS);
			root.addServlet(new ServletHolder(new MasterServlet()), "/*");
			server.start();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
