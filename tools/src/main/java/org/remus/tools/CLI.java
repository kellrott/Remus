package org.remus.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import jline.ConsoleReader;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.TokenStream;
import org.apache.thrift.TException;
import org.remus.RemusInterface;
import org.remus.RemusDatabaseException;
import org.remus.RemusRemote;
import org.remus.thrift.RemusNet;
import org.remus.thrift.RemusNet.Iface;
import org.remus.tools.antlr.RemusCliLexer;
import org.remus.tools.antlr.RemusCliParser;

public class CLI extends CLIInterface {

	private RemusInterface remus;
	private String curPipeline = null;
	private ConsoleReader reader;

	public CLI(RemusInterface remus) {
		this.remus = remus;
	}
	
	public void start() throws IOException {
		reader = new ConsoleReader();		
		do {
			String userInput = null;
			if (curPipeline == null) {
				userInput = reader.readLine("remus>");
			} else {
				userInput = reader.readLine("remus:" + curPipeline + ">");
			}
			if (userInput.length() > 0) {
				exec(userInput);
			}
		} while (!quit);
	}		


	public static void main(String [] args) throws Exception {	
		Map params = new HashMap();
		RemusInterface remote = RemusInterface.wrap(new RemusRemote(args[0], Integer.parseInt(args[1])));
		System.err.println("Connecting: " + args[0] + ":" + args[1]);
		CLI cli = new CLI(remote);
		cli.start();
	}

	public void println(String string) throws IOException {
		reader.printString(string);
		reader.printNewline();
	}

	public void changePipeline(String pipelineName) {
		curPipeline = pipelineName;
	}

	@Override
	public RemusInterface getRemusDB() {
		return remus;
	}

}
