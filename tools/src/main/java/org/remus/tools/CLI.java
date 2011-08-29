package org.remus.tools;

import java.io.Console;
import java.util.HashMap;
import java.util.Map;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.remus.plugin.PluginManager;
import org.remus.tools.antlr.RemusCliLexer;
import org.remus.tools.antlr.RemusCliParser;

public class CLI {

	private PluginManager pm;
	public CLI(PluginManager pm) {
		this.pm = pm;
	}

	public void start() {
		Console console = System.console();
		boolean quit = false;
		do {
			console.printf("remus> ");
			String userInput = console.readLine();		
			ANTLRStringStream sstream = new ANTLRStringStream(userInput);
			RemusCliLexer lex = new RemusCliLexer(sstream);
			CommonTokenStream tokens = new CommonTokenStream(lex);
			RemusCliParser parser = new RemusCliParser(tokens);
			try { 
				CLICommand cmd = parser.cmd();
				if (cmd.getType() == CLICommand.QUIT) {
					quit = true;	
				} else {
					cmd.runCommand(pm, console.writer());
				}
			} catch (Exception e) {
				e.printStackTrace();
				console.printf("UNKNOWN COMMAND\n");
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





}
