package org.remus.core;

import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;
import org.remus.thrift.RemusNet.Iface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AppletInstanceStack implements BaseStackNode {

	private RemusApp app;
	private String pipeline;
	TreeMap<String, String> aiMap;
	private RemusNet.Iface datastore;
	private Logger logger;
	private RemusNet.Iface attachstore; 

	public AppletInstanceStack(RemusNet.Iface db, RemusNet.Iface attach, String pipeline) {
		try {
			logger = LoggerFactory.getLogger(AppletInstanceStack.class);
			datastore = db;
			this.attachstore = attach;
			app = new RemusApp(datastore, attachstore);
			aiMap = new TreeMap<String, String>();
			this.pipeline = pipeline;
			loadPipeline(pipeline);
		} catch (RemusDatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void reset() {
		aiMap.clear();
	}

	void loadPipeline(String pipeline) {
		try {			
			logger.debug("Loading AppletStack: " + pipeline);
			RemusPipeline pipe = app.getPipeline(pipeline);
			TreeMap<String, String> aiList = new TreeMap<String, String>();		
			for (String appletName : pipe.getMembers()) {
				RemusApplet applet = pipe.getApplet(appletName);
				if (applet != null) {
					for (RemusInstance inst : applet.getInstanceList()) {
						AppletInstance ai = new AppletInstance(pipe, inst, applet, RemusDB.wrap(datastore));
						aiList.put(inst.toString() + ":" + applet.getID(), JSON.dumps(ai.getInstanceInfo()));
					}
				}
			}
			aiMap = aiList;
		} catch (RemusDatabaseException e) {
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	abstract public void add(String key, String data);

	@Override
	public void delete(String key) {
		try {
			RemusPipeline pipe = app.getPipeline(pipeline);
			String [] tmp = key.split(":");
			RemusApplet applet = null;
			if (tmp.length == 2) {
				applet = pipe.getApplet(tmp[1]);
			} else if (tmp.length == 3) {
				applet = pipe.getApplet(tmp[1] + ":" + tmp[2]);				
			}
			RemusInstance inst = RemusInstance.getInstance(datastore, pipeline, tmp[0]);
			applet.deleteInstance(inst);
		} catch (RemusDatabaseException  e) {
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean containsKey(String key) {
		if (!aiMap.containsKey(key)) {
			return false;
		}
		return true;
	}

	@Override
	public List<String> getValueJSON(String key) {
		List<String> out = new LinkedList<String>();
		if (aiMap != null) {
			if (aiMap.containsKey(key)) {
				out.add(aiMap.get(key));
			}
		}
		return out;
	}

	@Override
	public List<String> keySlice(String keyStart, int count) {
		TreeMap<String, String> o = aiMap;
		if (o != null) {
			NavigableSet<String> a = o.navigableKeySet();
			SortedSet<String> t = a.tailSet(keyStart);
			LinkedList<String> out = new LinkedList<String>();
			for (String name : t) {
				if (out.size() < count) {
					out.add(name);
				}
			}
			return out;
		}
		return null;
	}

}
