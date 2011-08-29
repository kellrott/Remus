package org.remus.tools;

import java.io.PrintWriter;

import org.apache.thrift.TException;
import org.remus.plugin.PluginManager;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;

public class CLICommand {

	public static final int QUIT = 1;
	public static final int LIST = 2;
	private int type;	
	
	public CLICommand(int cmdType) {
		this.type = cmdType;
	}

	public int getType() {
		return type;
	}

	public void runCommand(PluginManager pm, PrintWriter printWriter) throws NotImplemented, TException {
		if (type == LIST) {
			for (PeerInfoThrift info : pm.getIDServer().getPeers()) {
				printWriter.println(info.name + "\t" + info.peerID);				
			}
		}
	}

}
