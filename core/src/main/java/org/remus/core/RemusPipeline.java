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

import org.apache.thrift.TException;
import org.remus.KeyValPair;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;

import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;
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



	public void putApplet(RemusPipeline pipe, String name, Object data) throws TException, NotImplemented { 
		AppletRef ar = new AppletRef(pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
		datastore.add(ar, 0L, 0L, name, data);
	}	


	public Set<AppletInstance> getActiveApplets( ) throws TException, NotImplemented, RemusDatabaseException {
		Set<AppletInstance> out = new HashSet<AppletInstance>();
		for ( String appletName : getMembers() ) {
			RemusApplet applet = new RemusApplet(this, appletName, datastore, attachStore);
			out.addAll(applet.getActiveApplets());
		}
		return out;
	}


	public Collection<String> getMembers() {
		List<String> out = new LinkedList<String>();
		AppletRef arPipeline = new AppletRef(name, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
		for (String key : datastore.listKeys(arPipeline)) {
			out.add(key);
		}
		return out;
	}


	public boolean hasApplet(String appletName) {
		AppletRef arPipeline = new AppletRef( name, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" );
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

	public RemusApplet getApplet(String name) throws RemusDatabaseException {
		AppletRef arPipeline = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
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

	public String getSubKey(RemusInstance inst) {
		AppletRef arInstance = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance");
		try {
			for (Object instObject : datastore.get(arInstance, inst.toString())) {
				return (String) instObject;
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		return null;
	}

	public Map getSubmitData(String subKey) {
		AppletRef arSubmit = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");
		try {
			for (Object subObject : datastore.get(arSubmit, subKey)) {
				Map subMap = (Map) subObject;
				return subMap;
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
			AppletRef arInstance = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance");
			datastore.deleteValue(arInstance, instance.toString());
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}

	private List<RemusApplet> loadApplet(String pipelineName, String name, RemusDB store ) throws TException, NotImplemented, RemusDatabaseException {
		List<RemusApplet> out = new LinkedList<RemusApplet>();

		AppletRef arPipeline = new AppletRef(pipelineName, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
		Map appletObj = null;
		for (Object obj : store.get(arPipeline, name)) {
			appletObj = (Map) obj;
		}
		RemusApplet applet = new RemusApplet(this, name, datastore, attachStore);

		if (appletObj.containsKey(RemusApplet.OUTPUT_FIELD)) {
			for (Object nameObj : (List) appletObj.get(RemusApplet.OUTPUT_FIELD)) {
				RemusApplet outApplet = new RemusApplet(this, name + "." + (String) nameObj, datastore, attachStore);
				outApplet.setMode(RemusApplet.OUTPUT);
				for (String input : applet.getInputs()) {
					outApplet.addInput(input);
				}

				out.add(outApplet);
			}
		}		
		out.add(applet);

		return out;
	}

	public void deleteApplet(RemusApplet applet) throws TException, NotImplemented {		
		for (RemusInstance inst : applet.getInstanceList()) {
			applet.deleteInstance(inst);
		}
		AppletRef arPipeline = 
				new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID());
		datastore.deleteStack(arPipeline);
	}
	/*
	public boolean isComplete(RemusInstance inst) {
		boolean done = true;
		for ( RemusAppletImpl applet : members.values() ) {
			if ( ! WorkStatus.isComplete(applet, inst) )
				done = false;
		}
		return done;
	}
	 */



	public String getID() {
		return name;
	}


	public Iterable<KeyValPair> getSubmits() {
		AppletRef arSubmit = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");
		return datastore.listKeyPairs(arSubmit);
	}

	public void deleteSubmission(String key) throws TException, NotImplemented, RemusDatabaseException {
		AppletRef arSubmit = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");
		Map sMap = null;
		for (Object obj : datastore.get(arSubmit, key)) {
			sMap = (Map) sMap;
		}
		if (sMap != null) {
			String instStr = (String) sMap.get(PipelineSubmission.InstanceField);
			RemusInstance inst = new RemusInstance(instStr);
			deleteInstance(inst);
		}
		datastore.deleteValue(arSubmit, key);
	}

	public RemusInstance handleSubmission(String key, PipelineSubmission value) {

		RemusInstance inst;

		inst = setupInstance(key, value, value.getInitApplets());
		//only add the main submission/instance records if they don't already exist
		//we've already fired off the setupInstance requests to the applets, so if new applets are
		//to be instanced in an exisiting pipeline instance, they will be, but the original submisison 
		//will remain

		AppletRef arSubmit = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");

		try {
			//if (!datastore.containsKey(arSubmit, key)) {
				value.setSubmitKey(key);
				value.setInstance(inst);
				datastore.add(arSubmit,
						(Long) 0L,
						(Long) 0L,
						key,
						value);
			//}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		AppletRef arInstance = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance");

		try {
			datastore.add(arInstance,
					0L, 0L,
					inst.toString(),
					key);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		return inst;
	}


	public RemusInstance setupInstance(String name, PipelineSubmission params, List<String> appletList) {
		logger.info("Init submission " + name);
		Set<String> activeSet = new HashSet<String>();
		RemusInstance inst = new RemusInstance();

		AppletRef arSubmit = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");

		try {
			for (Object subObject : datastore.get(arSubmit, name)) {
				inst = new RemusInstance((String) ((Map) subObject).get(PipelineSubmission.InstanceField));
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}

		for (String appletName : appletList) {
			try {
				RemusApplet applet = getApplet(appletName);
				if (applet != null) {
					activeSet.add(appletName);
					try {
						applet.createInstance(name, params.base, inst);
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (NotImplemented e) {
						e.printStackTrace();
					}
				} else {
					logger.error("Can't find applet " + appletName);
				}
			} catch (RemusDatabaseException e) {
				logger.error("Bad applet in database");
			}
		}

		boolean added = false;
		do {
			added = false;
			for (String appletName : getMembers()) {
				if (!activeSet.contains(appletName)) {
					try {
						RemusApplet applet = getApplet((String) appletName);
						if (applet != null) {
							for (String iRef : applet.getInputs()) {
								if (iRef.compareTo("?") != 0) {
									RemusApplet srcApplet = getApplet(iRef);
									if (activeSet.contains(iRef)) {
										try {
											if (applet.createInstance(name, params.base, inst)) {
												added = true;
											}
										} catch (TException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										} catch (NotImplemented e) {
											e.printStackTrace();
										}
										activeSet.add(appletName);
									}
								}
							}
						}
					} catch (RemusDatabaseException e) {
						logger.error("Base database format");
					}
				}
			}
		} while (added);
		logger.info("submission " + name + " started as " + inst);
		return inst;		
	}


	public OutputStream writeAttachment(String name) throws IOException {
		AppletRef stack = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
		return attachStore.writeAttachment(stack, null, name);
	}

	public InputStream readAttachment(String name) throws NotImplemented {
		AppletRef stack = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
		return attachStore.readAttachement(stack, null, name);
	}

	public boolean hasAttachment(String name) throws NotImplemented, TException {
		AppletRef stack = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
		return attachStore.hasAttachment(stack, null, name);
	}



	public List<String> listAttachments() throws NotImplemented, TException {
		AppletRef stack = new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
		return attachStore.listAttachments(stack, null);
	}

}
