package org.remus.tools;

import java.io.IOException;

import org.apache.thrift.TException;
import org.remus.RemusDatabaseException;
import org.remus.core.RemusApp;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PluginManager;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;

public class CLICommand {

	public static final int QUIT = 1;
	public static final int SHOW = 2;
	public static final int USE = 3;

	public static final int SERVERS = 1;
	public static final int LIST = 2;
	public static final int PIPELINES = 3;
	public static final int INSTANCES = 4;
	public static final int APPLETS = 5;

	private int type;	
	private String pipelineName;
	private int system;

	public CLICommand(int cmdType) {
		this.type = cmdType;
	}

	public void setSystem(int system) {
		this.system = system;
	}

	public int getType() {
		return type;
	}

	public void runCommand(PluginManager pm, CLI cli) throws NotImplemented, TException, RemusDatabaseException, IOException {
		switch (type) {
		case SHOW: {
			switch (system) {
			case SERVERS: {
				for (PeerInfoThrift info : pm.getIDServer().getPeers()) {
					cli.println(info.name + "\t" + info.peerID);				
				}
			}
			break;
			case PIPELINES:{
				RemusApp app = cli.getRemusApp();
				for (String name : app.getPipelines() ) {
					cli.println(name);
				}
			}
			break;
			case INSTANCES: {
				RemusPipeline pipe = cli.getPipeline();
				if (pipe != null) {
					for (String appletName : pipe.getMembers()) {
						RemusApplet applet = pipe.getApplet(appletName);
						for (RemusInstance inst : applet.getInstanceList()) {
							cli.println(inst.toString() + ":" + appletName);
						}
					}
				}
			}
			break;
			case APPLETS: {
				RemusPipeline pipe = cli.getPipeline();
				if (pipe != null) {
					for (String member : pipe.getMembers()) {
						cli.println(member);
					}
				}
			}
			break;
			}	
		}
		break;
		case USE: {
			cli.changePipeline(pipelineName);
		}
		break;
		}
	}

	public void setPipeline(String pn) {
		pipelineName = pn;
	}

}
