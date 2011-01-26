package org.remus;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class MasterServer {

	public static void main(String args[]) {
		try {
			Server server = new Server(16016);
			Context root = new Context(server,"/",Context.SESSIONS);
			ServletHolder sh = new ServletHolder(new MasterServlet());
			sh.setInitParameter("org.remus.mpstore", "org.mpstore.SQLStore");
			sh.setInitParameter("org.remus.srcdir", args[0] );
			sh.setInitParameter("org.remus.workdir", args[1] );
			root.addServlet(sh, "/*");
			server.start();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
