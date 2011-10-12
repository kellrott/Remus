package org.remus.core;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.thrift.TException;
import org.remus.ConnectionException;
import org.remus.PeerInfo;
import org.remus.RemusDB;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerType;

public class MemoryDB extends RemusDB {
	class StackRow {
		Map<String,String> baseMap = new HashMap<String, String>();		
		public void put(long jobID, long emitID, String data) {
			baseMap.put(Long.toString(jobID) + "_" + Long.toString(emitID), data);
		}		

		@Override
		public String toString() {
			return baseMap.toString();
		}
	}

	class StackMap {
		TreeMap<String, StackRow> baseMap = new TreeMap<String, StackRow>();

		long timestamp = 0;

		public void addData(long jobID, long emitID, String key, String data) {
			timestamp = (new Date()).getTime();
			StackRow sr = baseMap.get(key);
			if (sr == null) {
				sr = new StackRow();
				baseMap.put(key, sr);
			}
			sr.put(jobID, emitID, data);
		}

		public boolean containsKey(String key) {
			return baseMap.containsKey(key);
		}

		public void remove(String key) {
			baseMap.remove(key);
		}

		public long getTimeStamp() {
			return timestamp;
		}

		public StackRow get(String key) {
			return baseMap.get(key);
		}

		@Override
		public String toString() {
			return baseMap.toString();		
		}

	}

	TreeMap<String,StackMap> baseMap;

	@Override
	public void init(Map params) throws ConnectionException {
		baseMap = new TreeMap<String,StackMap>();		
	}

	private String stackName(AppletRef stack) {
		return stack.pipeline + "/" + stack.instance + "/" + stack.applet;
	}

	private AppletRef stackApplet(String name) {
		String [] tmp = name.split("/");
		return new AppletRef(tmp[0], tmp[1], tmp[2]);
	}	
		
	@Override
	public void addDataJSON(AppletRef stack, long jobID, long emitID, String key,
			String data) throws NotImplemented, TException {

		String sn = stackName(stack);
		StackMap s = baseMap.get(sn);

		if (s == null) {
			s = new StackMap();
			baseMap.put(sn, s);
		}		
		s.addData(jobID, emitID, key, data);
	}

	@Override
	public boolean containsKey(AppletRef stack, String key)
			throws NotImplemented, TException {
		String sn = stackName(stack);
		StackMap s = baseMap.get(sn);
		if (s != null) {
			return s.containsKey(key);
		}
		return false;
	}

	@Override
	public void deleteStack(AppletRef stack) throws NotImplemented, TException {
		String sn = stackName(stack);
		baseMap.remove(sn);
	}

	@Override
	public void deleteValue(AppletRef stack, String key) throws NotImplemented,
	TException {
		String sn = stackName(stack);
		StackMap s = baseMap.get(sn);
		if (s != null) {
			s.remove(key);
		}
	}

	@Override
	public long getTimeStamp(AppletRef stack) throws NotImplemented, TException {
		String sn = stackName(stack);
		StackMap s = baseMap.get(sn);
		if (s != null) {
			return s.getTimeStamp();
		}
		return 0;
	}

	@Override
	public List<String> getValueJSON(AppletRef stack, String key)
			throws NotImplemented, TException {
		String sn = stackName(stack);
		StackMap s = baseMap.get(sn);
		if (s != null) {
			if (s.containsKey(key)) {
				return new ArrayList(s.get(key).baseMap.values());
			}
		}
		return new ArrayList<String>();
	}

	@Override
	public long keyCount(AppletRef stack, int maxCount) throws NotImplemented,
	TException {
		String sn = stackName(stack);
		StackMap s = baseMap.get(sn);
		if (s != null) {
			return s.baseMap.size();
		}
		return 0;
	}

	@Override
	public List<String> keySlice(AppletRef stack, String keyStart, int count)
			throws NotImplemented, TException {		
		String sn = stackName(stack);
		StackMap s = baseMap.get(sn);
		if (s != null) {
			NavigableSet<String> a = s.baseMap.navigableKeySet();
			SortedSet<String> t = a.tailSet(keyStart);
			LinkedList<String> out = new LinkedList<String>();
			for (String name : t) {
				if (out.size() < count) {
					out.add(name);
				}
			}
			return out;
		}		
		return new LinkedList<String>();
	}


	@Override
	public List<KeyValJSONPair> keyValJSONSlice(AppletRef stack, String startKey, int count) 
			throws NotImplemented, TException {
		String sn = stackName(stack);
		StackMap s = baseMap.get(sn);
		if (s != null) {
			NavigableSet<String> a = s.baseMap.navigableKeySet();
			SortedSet<String> t = a.tailSet(startKey);
			LinkedList<KeyValJSONPair> out = new LinkedList<KeyValJSONPair>();
			int i = 0;
			for (String key : t) {
				if (i < count) {
					StackRow row = s.get(key);
					for (Entry<String, String> e : row.baseMap.entrySet()) {
						String [] tmp = e.getKey().split("_");
						long jobID = Long.parseLong(tmp[0]);
						long emitID = Long.parseLong(tmp[1]);
						out.add(new KeyValJSONPair(key, e.getValue(), jobID, emitID));
					}
				}
				i++;
			}
			return out;
		}		
		return new ArrayList<KeyValJSONPair>();
	}

	
	@Override
	public List<AppletRef> stackSlice(String startKey, int count)
			throws NotImplemented, TException {		
		NavigableSet<String> a = baseMap.navigableKeySet();
		SortedSet<String> t = a.tailSet(startKey);
		LinkedList<AppletRef> out = new LinkedList<AppletRef>();
		for (String name : t) {
			if (out.size() < count) {
				out.add(stackApplet(name));
			}
		}
		return out;
	}


	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.peerType = PeerType.DB_SERVER;
		out.name = "Remus Memory DB";
		return out;
	}

	@Override
	public void start(PluginManager pluginManager) throws Exception {}

	@Override
	public void stop() {}

	@Override
	public String toString() {
		return baseMap.toString();
	}
	
}
