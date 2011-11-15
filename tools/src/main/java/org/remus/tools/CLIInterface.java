package org.remus.tools;

import java.io.IOException;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.TokenStream;
import org.apache.thrift.TException;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.core.RemusApp;
import org.remus.core.RemusPipeline;
import org.remus.thrift.RemusNet;
import org.remus.tools.antlr.RemusCliLexer;
import org.remus.tools.antlr.RemusCliParser;

public abstract class CLIInterface {

	Boolean quit = false;
	
	public void exec(String command) throws IOException {
		ANTLRStringStream sstream = new ANTLRStringStream(command);
		RemusCliLexer lex = new RemusCliLexer(sstream);
		TokenStream tokens = new CommonTokenStream(lex);
		RemusCliParser parser = new RemusCliParser(tokens);
		try { 
			if (!parser.failed()) {
				CLICommand cmd = parser.cmd();
				if (cmd.getType() == CLICommand.QUIT) {
					quit = true;	
				} else {
					cmd.runCommand(this);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			println("UNKNOWN COMMAND");
		}		
	}
	
	
	public abstract RemusApp getRemusApp() throws RemusDatabaseException, TException;
	public abstract void changePipeline(String pipelineName);
	public abstract RemusPipeline getPipeline()  throws RemusDatabaseException, TException;
	public abstract RemusDB getDataSource()  throws RemusDatabaseException, TException;
	public abstract RemusAttach getAttachStore()  throws RemusDatabaseException, TException;
	public abstract void println(String string) throws IOException;
	public abstract RemusNet.Iface getManager() throws TException;
}
