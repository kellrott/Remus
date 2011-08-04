package org.remus.js;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.core.BaseNode;
import org.remus.core.WorkAgent;
import org.remus.core.WorkManager;
import org.remus.core.WorkStatus;
import org.remus.plugin.PluginInterface;
import org.remus.plugin.PluginManager;
import org.remus.thrift.PeerType;
import org.remus.thrift.WorkDesc;
import org.remus.PeerInfo;

public class JSWorker implements WorkAgent {

	
	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.peerType = PeerType.WORKER;
		out.workTypes =  Arrays.asList("javascript");
		return out;
	}

	@Override
	public void init(Map params) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String jobRequest(String dataServer, WorkDesc work)
			throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public org.remus.thrift.WorkStatus workStatus(String jobID)
			throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void start(PluginManager pluginManager) {
		// TODO Auto-generated method stub
		
	}
	

}
