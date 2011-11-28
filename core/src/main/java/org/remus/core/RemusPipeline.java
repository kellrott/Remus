package org.remus.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

import org.apache.thrift.TException;
import org.remus.KeyValPair;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;

import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemusPipeline {

	//public static final String CODE_FIELD = "_code";

	String name;
	RemusDB datastore;
	RemusAttach attachStore;
	RemusApp app;

	private Logger logger;

	public RemusPipeline(RemusApp app, String name, RemusDB datastore, RemusAttach attachStore) {
		logger = LoggerFactory.getLogger(RemusPipeline.class);	
		this.name = name;
		this.app = app;
		this.datastore = datastore;
		this.attachStore = attachStore;
	}



	public void putApplet(String name, Object data) throws TException, NotImplemented { 
		AppletRef ar = new AppletRef(getID(), Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET);
		datastore.add(ar, 0L, 0L, name, data);
	}	


	/*
	public Set<AppletInstance> getActiveApplets(RemusInstance inst) throws TException, NotImplemented, RemusDatabaseException {
		Set<AppletInstance> out = new HashSet<AppletInstance>();
		for (String appletName : getMembers()) {
			RemusApplet applet = new RemusApplet(this, appletName, datastore, attachStore);
			AppletRef arInst = new AppletRef(name, Constants.STATIC_INSTANCE, Constants.INSTANCE_APPLET);
			if (datastore.containsKey(arInst, inst.toString() +":" + appletName) ) {
				out.addAll(applet.getActiveApplets(inst));
			}
		}
		return out;
	}
	*/


	public Collection<String> getMembers() {
		List<String> out = new LinkedList<String>();
		AppletRef arPipeline = new AppletRef(name, Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET);
		for (String key : datastore.listKeys(arPipeline)) {
			out.add(key);
		}
		return out;
	}


	public boolean hasApplet(String appletName) {
		AppletRef arPipeline = new AppletRef(name, Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET);
		try {
			return datastore.containsKey(arPipeline, appletName);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	/*
	public void setInstanceSubkey(RemusInstance instance, String key) throws TException, NotImplemented {
		AppletRef arInst = new AppletRef(name, Constants.STATIC_INSTANCE, Constants.INSTANCE_APPLET);
		datastore.add(arInst, 0, 0, instance.toString(), key);
	}
	 */
	
	public AppletInstance getAppletInstance( RemusInstance inst, String applet) throws RemusDatabaseException {
		if (hasAppletInstance(inst, applet)) {
			return new AppletInstance(this.getID(), inst, applet, datastore, attachStore);
		}
		return null;
	}


	public boolean hasAppletInstance(RemusInstance instance, String applet) {
		AppletRef arInst = new AppletRef(name, Constants.STATIC_INSTANCE, Constants.INSTANCE_APPLET);
		try {
			String [] tmp = applet.split(":");
			return datastore.containsKey(arInst, instance.toString() + ":" + tmp[0]);
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}


	public RemusApplet getApplet(String name) throws RemusDatabaseException {
		AppletRef arPipeline = new AppletRef(getID(), Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET);
		try {
			if (datastore.containsKey(arPipeline, name)) {
				return new RemusApplet(this, name, datastore, attachStore);
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		return null;
	}

	public RemusInstance getInstance(String name) {
		try {
			RemusInstance inst = RemusInstance.getInstance(datastore, getID(), name);
			return inst;
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		return null;
	}

	
	public PipelineSubmission getSubmitData(String subKey) {
		AppletRef arSubmit = new AppletRef(getID(), Constants.STATIC_INSTANCE, Constants.SUBMIT_APPLET);
		try {
			for (Object subObject : datastore.get(arSubmit, subKey)) {
				Map subMap = (Map) subObject;
				return new PipelineSubmission(subMap);
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		return null;
	}	

	public void deleteInstance(RemusInstance instance) throws RemusDatabaseException {
		try { 
			logger.info("Deleting Instance " + instance);
			for (String appletName : getMembers()) {
				RemusApplet applet = new RemusApplet(this, appletName, datastore, attachStore);
				applet.deleteInstance(instance);
			}
			Set<String> removeSet = new HashSet<String>();
			AppletRef arInstance = new AppletRef(getID(), Constants.STATIC_INSTANCE, Constants.INSTANCE_APPLET);
			for (String aiName : datastore.listKeys(arInstance) ) {
				if (aiName.startsWith(instance.toString())) {
					removeSet.add(aiName);
				}
			}
			for (String key : removeSet) {
				datastore.deleteValue(arInstance, key);
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}


	public void deleteApplet(RemusApplet applet) throws TException, NotImplemented {		
		AppletRef arPipeline = new AppletRef(getID(), Constants.STATIC_INSTANCE, Constants.PIPELINE_APPLET);
		datastore.deleteValue(arPipeline, applet.getID());		
	}

	public String getID() {
		return name;
	}


	public Iterable<KeyValPair> getSubmitValues() {
		AppletRef arSubmit = new AppletRef(getID(), Constants.STATIC_INSTANCE, Constants.SUBMIT_APPLET);
		return datastore.listKeyPairs(arSubmit);
	}

	public void deleteSubmission(String key) throws TException, NotImplemented, RemusDatabaseException {
		AppletRef arSubmit = new AppletRef(getID(), Constants.STATIC_INSTANCE, Constants.SUBMIT_APPLET);
		Map sMap = null;
		for (Object obj : datastore.get(arSubmit, key)) {
			sMap = (Map) sMap;
		}
		if (sMap != null) {
			String instStr = (String) sMap.get(PipelineSubmission.INSTANCE_FIELD);
			RemusInstance inst = new RemusInstance(instStr);
			deleteInstance(inst);
		}
		datastore.deleteValue(arSubmit, key);
	}

	public Iterable<String> getSubmits() {
		AppletRef arSubmit = new AppletRef(getID(), Constants.STATIC_INSTANCE, Constants.SUBMIT_APPLET);
		return datastore.listKeys(arSubmit);
	}

	public void setSubmit(String subKey, PipelineSubmission subData) throws TException, NotImplemented {
		AppletRef arSubmit = new AppletRef(getID(), Constants.STATIC_INSTANCE, Constants.SUBMIT_APPLET);
		datastore.add(arSubmit,
				(Long) 0L,
				(Long) 0L,
				subKey,
				subData);
	}



	public Set<AppletInstance> getAppletInstanceList() {
		Set<AppletInstance> out = new HashSet<AppletInstance>();
		AppletRef arInstance = new AppletRef(getID(), Constants.STATIC_INSTANCE, Constants.INSTANCE_APPLET);
		for ( KeyValPair kv : datastore.listKeyPairs(arInstance)) {
			out.add( new AppletInstance(new AppletInstanceRecord(kv.getValue()), datastore, attachStore) );
		}
		return out;
	}

}
