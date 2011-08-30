package org.remus.tools;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import jline.ConsoleReader;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.TokenStream;
import org.remus.RemusDatabaseException;
import org.remus.core.RemusApp;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PluginManager;
import org.remus.tools.antlr.RemusCliLexer;
import org.remus.tools.antlr.RemusCliParser;

public class CLI {

	private PluginManager pm;
	private String curPipeline = null;
	private ConsoleReader reader;
	public CLI(PluginManager pm) {
		this.pm = pm;
	}

	public void start() throws IOException {

		reader = new ConsoleReader();
		
		boolean quit = false;
		do {
			String userInput = null;
			if (curPipeline == null) {
				userInput = reader.readLine("remus>");
			} else {
				userInput = reader.readLine("remus:" + curPipeline + ">");
			}
			ANTLRStringStream sstream = new ANTLRStringStream(userInput);
			RemusCliLexer lex = new RemusCliLexer(sstream);
			TokenStream tokens = new CommonTokenStream(lex);
			RemusCliParser parser = new RemusCliParser(tokens);
			try { 
				if (!parser.failed()) {
					CLICommand cmd = parser.cmd();
					if (cmd.getType() == CLICommand.QUIT) {
						quit = true;	
					} else {
						cmd.runCommand(pm, this);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				reader.printString("UNKNOWN COMMAND\n");
			}
		} while (!quit);
	}		


	public static void main(String [] args) throws Exception {	
		Map params = new HashMap();
		Map conf = new HashMap();
		Map client  = new HashMap();
		client.put("host", args[0]);
		client.put("port", Integer.parseInt(args[1]));
		conf.put("client", client);		
		params.put("org.remus.RemusIDClient", conf);

		PluginManager pm = new PluginManager(params);				
		pm.start();
		CLI cli = new CLI(pm);
		cli.start();		
		pm.close();
	}

	public void println(String string) throws IOException {
		reader.printString(string);
		reader.printNewline();
	}

	public void changePipeline(String pipelineName) {
		curPipeline = pipelineName;
	}

	public RemusApp getRemusApp() throws RemusDatabaseException {
		RemusApp app = new RemusApp(pm.getPeer(pm.getDataServer()), pm.getPeer(pm.getAttachStore()));
		return app;
	}

	public RemusPipeline getPipeline() throws RemusDatabaseException {
		if (curPipeline != null) {
			RemusApp app = getRemusApp();
			return app.getPipeline(curPipeline);
		}
		return null;
	}

}
