package org.remus.manage;

import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.core.AppletInstance;
import org.remus.core.BaseStackNode;
import org.remus.core.RemusApp;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PeerManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppletInstanceStack implements BaseStackNode {

	private RemusApp app;
	TreeMap<String, TreeMap<String, String>> aiMap;
	private RemusNet.Iface datastore;
	private Logger logger; 
	
	public AppletInstanceStack(PeerManager plugins) {
		try {
			logger = LoggerFactory.getLogger(AppletInstanceStack.class);
			datastore = plugins.getPeer(plugins.getDataServer());
			app = new RemusApp(datastore, null);
			aiMap = new TreeMap<String, TreeMap<String, String>>();
		} catch (RemusDatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void reset() {
		aiMap.clear();
	}

	void loadPipeline(AppletRef stack) {
		try {
			if (aiMap.containsKey(stack.pipeline)) {
				return;
			}
			logger.debug("Loading AppletStack: " + stack.pipeline);
			RemusPipeline pipe = app.getPipeline(stack.pipeline);
			TreeMap<String, String> aiList = new TreeMap<String, String>();		
			for (String appletName : pipe.getMembers()) {
				RemusApplet applet = pipe.getApplet(appletName);
				for (RemusInstance inst : applet.getInstanceList()) {
					AppletInstance ai = new AppletInstance(pipe, inst, applet, RemusDB.wrap(datastore));
					aiList.put(inst.toString() + ":" + applet.getID(), JSON.dumps(ai.getInstanceInfo()));
				}
			}
			aiMap.put(stack.pipeline, aiList);
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
	public void add(AppletRef stack, long jobID, long emitID, String key, String data) {
		logger.info("Adding Instance:" + key + " " + data); 
	}

	@Override
	public boolean containsKey(AppletRef stack, String key) {
		loadPipeline(stack);
		if (!aiMap.containsKey(stack.pipeline) || !aiMap.get(stack.pipeline).containsKey(key)) {
			return false;
		}
		return true;
	}

	@Override
	public List<String> getValueJSON(AppletRef stack, String key) {
		loadPipeline(stack);
		List<String> out = new LinkedList<String>();
		TreeMap<String, String> o = aiMap.get(stack.pipeline);
		if (o != null) {
			if (o.containsKey(key)) {
				out.add(o.get(key));
			}
		}
		return out;
	}

	@Override
	public List<String> keySlice(AppletRef stack, String keyStart, int count) {
		loadPipeline(stack);
		TreeMap<String, String> o = aiMap.get(stack.pipeline);
		if (o != null) {
			NavigableSet<String> a = o.descendingKeySet();
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
