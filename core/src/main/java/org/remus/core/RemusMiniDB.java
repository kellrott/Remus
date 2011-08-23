package org.remus.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.ConnectionException;
import org.remus.PeerInfo;
import org.remus.RemusDB;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;
import org.remus.thrift.RemusNet.Iface;

public class RemusMiniDB extends RemusDB {

	private Iface base;
	private Map<String, BaseStackNode> nodes;
	public RemusMiniDB(RemusNet.Iface base) {
		this.base = base;
		nodes = new HashMap<String, BaseStackNode>();
	}

	public void addBaseStack(String applet, BaseStackNode node) {
		nodes.put(applet, node);
	}

	public void delBaseStack(String applet) {
		nodes.remove(applet);
	}

	@Override
	public void init(Map params) throws ConnectionException {}

	@Override
	public void addData(AppletRef stack, long jobID, long emitID, String key,
			String data) throws NotImplemented, TException {
		if (nodes.containsKey(stack.applet)) {
			nodes.get(stack.applet).add(stack, jobID, emitID, key, data);
		} else {
			base.addData(stack, jobID, emitID, key, data);
		}
	}

	@Override
	public boolean containsKey(AppletRef stack, String key)
	throws NotImplemented, TException {
		if (nodes.containsKey(stack.applet)) {
			return nodes.get(stack.applet).containsKey(stack, key);
		} 
		return base.containsKey(stack, key);
	}

	@Override
	public void deleteStack(AppletRef stack) throws NotImplemented, TException {
		throw new NotImplemented();
	}

	@Override
	public void deleteValue(AppletRef stack, String key) throws NotImplemented,
	TException {
		throw new NotImplemented();
	}

	@Override
	public long getTimeStamp(AppletRef stack) throws NotImplemented, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<String> getValueJSON(AppletRef stack, String key)
	throws NotImplemented, TException {
		if (nodes.containsKey(stack)) {
			return nodes.get(stack.applet).getValueJSON(stack,key);
		}
		return base.getValueJSON(stack, key);
	}

	@Override
	public long keyCount(AppletRef stack, int maxCount) throws NotImplemented,
	TException {
		throw new NotImplemented();
	}

	@Override
	public List<String> keySlice(AppletRef stack, String keyStart, int count)
	throws NotImplemented, TException {
		if (nodes.containsKey(stack.applet)) {
			return nodes.get(stack.applet).keySlice(stack,keyStart,count);
		}
		return base.keySlice(stack, keyStart, count);
	}

	@Override
	public List<KeyValJSONPair> keyValJSONSlice(AppletRef stack,
			String startKey, int count) throws NotImplemented, TException {
		throw new NotImplemented();
	}

	@Override
	public PeerInfo getPeerInfo() {
		return null;
	}

	@Override
	public void start(PluginManager pluginManager) throws Exception {}

	@Override
	public void stop() {}

}
