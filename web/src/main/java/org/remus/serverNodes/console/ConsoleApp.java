package org.remus.serverNodes.console;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.RemusWeb;
import org.remus.core.RemusApp;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.tools.CLIInterface;

public class ConsoleApp implements BaseNode {

	private RemusWeb web;

	public ConsoleApp(RemusWeb web) {
		this.web = web;
	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os)
			throws FileNotFoundException {
		if (name.length()==0) {
			name = "terminal.html";
		}

		try {
			InputStream rs = ConsoleApp.class.getResourceAsStream(name);
			if (rs != null) {
				int len;
				byte [] buffer = new byte[10240];
				while ((len=rs.read(buffer))>0) {
					os.write(buffer,0,len);
				}
				rs.close();
				os.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {

		try {
			StringBuilder sb = new StringBuilder();
			byte [] buffer = new byte[1024];
			int len;
			while ((len = is.read(buffer)) > 0) {
				sb.append(new String(buffer, 0, len));
			}
			System.err.println(sb.toString());
			Map data = (Map)JSON.loads(sb.toString());
			String pipelineName = null;
			if (data.containsKey("pipeline")) {
				pipelineName = (String)data.get("pipeline");
			}
			final String pipeline = pipelineName;
			final String command = (String)data.get("command");

			final PrintWriter pw = new PrintWriter(os);

			CLIInterface cli = new CLIInterface() {

				@Override
				public RemusApp getRemusApp() throws RemusDatabaseException,
				TException {
					return new RemusApp(web.getDataStore(), web.getAttachStore());
				}

				@Override
				public void changePipeline(String pipelineName) {

				}

				@Override
				public RemusPipeline getPipeline()
						throws RemusDatabaseException, TException {
					return getRemusApp().getPipeline(pipeline);
				}

				@Override
				public RemusDB getDataSource() throws RemusDatabaseException,
				TException {
					return web.getDataStore();
				}

				@Override
				public RemusAttach getAttachStore()
						throws RemusDatabaseException, TException {
					return web.getAttachStore();
				}

				@Override
				public void println(String string) throws IOException {
					pw.println(string);					
				}

			};
			cli.exec(command);
			pw.flush();
			os.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void doDelete(String name, Map params, String workerID)
			throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

}
