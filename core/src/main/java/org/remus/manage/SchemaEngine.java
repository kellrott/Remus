package org.remus.manage;

import java.io.IOException;
import java.util.List;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.core.AppletInstanceStack;
import org.remus.core.BaseStackNode;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusApp;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusMiniDB;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PeerManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaEngine {

	private RemusMiniDB miniDB;
	private PeerManager peerManager;
	private Logger logger;

	public SchemaEngine(PeerManager pm) throws TException {
		peerManager = pm;
		/**
		 * The miniDB is a shim placed infront of the database, that will allow you
		 * to add additional, dynamic, applets to the database. For the work manager
		 * it is used to create the '/@agent' applets, which view all the applet instance
		 * records as a single stack
		 */

		miniDB = new RemusMiniDB(peerManager.getPeer(peerManager.getDataServer()));
		logger = LoggerFactory.getLogger(SchemaEngine.class);
	}


	public void processSubmissions(RemusPipeline pipe) throws RemusDatabaseException, TException, NotImplemented, IOException {
		for (String subKey : pipe.getSubmits()) {
			PipelineSubmission subData = pipe.getSubmitData(subKey);
			if (subData != null) {
				if (checkSubmission(pipe, subKey, subData)) {
					pipe.setSubmit(subKey,subData);
					//pipe.setInstanceSubkey(subData.getInstance(), subKey);
				}
			}
			//instList.add(subData.getInstance());
		}	
	}


	public boolean checkSubmission(RemusPipeline pipe, String subKey, PipelineSubmission subData) 
			throws RemusDatabaseException, TException, NotImplemented, IOException {
		Boolean changed = false;
		
		//make sure the '_submitKey' field is correct
		if (!subData.hasSubmitKey() || subData.getSubmitKey().compareTo(subKey) != 0 ) {
			subData.setSubmitKey(subKey);
			changed = true;
		}
		
		//make sure the '_instance' field is correct
		if (!subData.hasInstance()) {
			subData.setInstance(new RemusInstance());
			changed = true;
		}

		//make sure the applets listed in '_submitInit'
		for (String applet : subData.getInitApplets()) {
			if (!pipe.hasAppletInstance(subData.getInstance(), applet)) {
				RemusApplet ap = pipe.getApplet(applet);
				if (ap != null) {
					ap.createInstance(subData, subData.getInstance());
				}
			}
		}	
		
		//if store tables have been created, make sure they have an instance record
		for (String appletName : pipe.getMembers()) {
			RemusApplet applet = pipe.getApplet(appletName);
			if (applet.getMode() == RemusApplet.STORE) {
				if (!applet.hasInstance(subData.getInstance())) {
					AppletRef dataRef = new AppletRef(pipe.getID(), subData.getInstance().toString(), applet.getID());
					
					if (miniDB.keyCount(dataRef, 1)!= 0) {
						applet.createInstance(subData, subData.getInstance());
						logger.info("Creating Instance record for data store:" + subData.getInstance() + " " + applet.getID() );
					}
				}
			}
		}
		
		return changed;
	}



	public void setupAIStack() throws RemusDatabaseException, TException {		
		RemusNet.Iface db = null;
		RemusNet.Iface attach = null;
		try {
			db = peerManager.getPeer(peerManager.getDataServer());
			attach = peerManager.getPeer(peerManager.getAttachStore());
			RemusApp app = new RemusApp(db, attach);		
			miniDB.reset();
			for (String pipeline : app.getPipelines()) {		
				AppletInstanceStack aiStack = new AppletInstanceStack(db, attach, pipeline) {
					@Override
					public void add(String key, String data) {}				
				};
				miniDB.addBaseStack("/@agent?" + pipeline, aiStack);
			}
		} finally {
			peerManager.returnPeer(attach);
			peerManager.returnPeer(db);
		}
	}


	public List<String> keySlice(AppletRef stack, String keyStart, int count) throws NotImplemented, TException {
		return miniDB.keySlice(stack, keyStart, count);
	}


	public boolean containsKey(AppletRef stack, String key) throws NotImplemented, TException {
		return miniDB.containsKey(stack, key);
	}


	public List<String> getValueJSON(AppletRef stack, String key) throws NotImplemented, TException {
		return miniDB.getValueJSON(stack, key);
	}


	public void addDataJSON(AppletRef stack, long jobID, long emitID,
			String key, String data) throws TException, NotImplemented {

		if (stack.applet.compareTo(Constants.SUBMIT_APPLET) == 0) {
			RemusNet.Iface db = null;
			RemusNet.Iface attach = null;
			try {
				db = peerManager.getPeer(peerManager.getDataServer());
				attach = peerManager.getPeer(peerManager.getAttachStore());
				PipelineSubmission subData = new PipelineSubmission(JSON.loads(data));
				RemusApp app = new RemusApp(RemusDB.wrap(db), RemusAttach.wrap(attach));
				RemusPipeline pipe = app.getPipeline(stack.pipeline);
				if (pipe != null) {
					if (checkSubmission(pipe, key, subData)) {
						pipe.setSubmit(key, subData);
						//pipe.setInstanceSubkey(subData.getInstance(), key);
					}	
				} 
			} catch (RemusDatabaseException e) {
				throw new TException(e);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				peerManager.returnPeer(attach);
				peerManager.returnPeer(db);
			}
		} else {			
			miniDB.addDataJSON(stack, jobID, emitID, key, data);
		}

	}


	public void addWorkStatus(BaseStackNode workStatusStack) {
		miniDB.addBaseStack(Constants.WORKSTAT_APPLET, workStatusStack);
	}



}
