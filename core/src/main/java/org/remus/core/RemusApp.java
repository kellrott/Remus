package org.remus.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;

import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;

/**
 * Main Interface to Remus applications. In charge of root database interface and pipeline 
 * management.
 * 
 * @author kellrott
 *
 */

public class RemusApp {

	RemusDB rootStore;
	RemusAttach rootAttachStore;

	public RemusApp(RemusNet.Iface datastore, RemusNet.Iface attachstore) throws RemusDatabaseException {
		rootStore = RemusDB.wrap(datastore);
		rootAttachStore = RemusAttach.wrap(attachstore);
		//scanSource(srcbase);
		try {
			getPipelines();
		} catch (TException e) {
			throw new RemusDatabaseException(e.toString());
		}
	}

	public void deletePipeline(RemusPipeline pipe) throws TException, RemusDatabaseException {		
		try {
			for (String appletName : pipe.getMembers()) {
				pipe.deleteApplet(pipe.getApplet(appletName));
			}
			rootStore.deleteValue(new AppletRef(Constants.ROOT_PIPELINE, Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET), pipe.getID());
			rootStore.deleteStack(new AppletRef(pipe.getID(), Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET));
			rootStore.deleteStack(new AppletRef(pipe.getID(), Constants.STATIC_INSTANCE, Constants.SUBMIT_APPLET));
			rootStore.deleteStack(new AppletRef(pipe.getID(), Constants.STATIC_INSTANCE, Constants.INSTANCE_APPLET));
			rootAttachStore.deleteStack(new AppletRef(pipe.getID(), Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET));
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}

	
	public void putPipeline(String pipelineName, PipelineDesc pDesc) throws TException, NotImplemented {
		AppletRef arPipeline = new AppletRef(Constants.ROOT_PIPELINE, Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET);
		rootStore.add(arPipeline, 0L, 0L, pipelineName, new HashMap());
		for (String appletName : pDesc.getApplets()) {
			AppletRef arApplet = new AppletRef(pipelineName, Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET);
			try {
				Map appletData = pDesc.getApplet(appletName);
				rootStore.add(arApplet, 0, 0, appletName, appletData);
			} catch (ClassCastException e) {}
		}		
	}
	

	public RemusPipeline getPipeline(String name) {
		if (hasPipeline(name)) {
			return new RemusPipeline(this, name, rootStore, rootAttachStore);
		} 
		return null;
	}

	public Collection<String> getPipelines() throws TException {
		AppletRef ar = new AppletRef(Constants.ROOT_PIPELINE, Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET);
		List out = new LinkedList();
		for (String key : rootStore.listKeys(ar)) {
			out.add(key);
		}
		return out;
	}


	public boolean hasPipeline(String name) {
		AppletRef ar = new AppletRef(Constants.ROOT_PIPELINE, Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET);
		try {
			return rootStore.containsKey(ar, name);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}
