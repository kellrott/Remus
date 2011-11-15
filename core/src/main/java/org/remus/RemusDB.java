package org.remus;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.plugin.PluginInterface;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.AttachmentInfo;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.JobStatus;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.RemusNet;
import org.remus.thrift.WorkDesc;

public abstract class RemusDB extends RemusPeer {

	
	public static RemusDB wrap(final RemusNet.Iface db) {
		if (db == null) {
			return null;
		}
		if (db instanceof RemusDB) {
			return (RemusDB) db;
		}
		return new RemusDB() {
			
			@Override
			public void init(Map params) throws ConnectionException {}

			@Override
			public void addDataJSON(AppletRef stack, long jobID, long emitID,
					String key, String data) throws NotImplemented, TException {
				db.addDataJSON(stack, jobID, emitID, key, data);
			}

			@Override
			public boolean containsKey(AppletRef stack, String key)
					throws NotImplemented, TException {
				return db.containsKey(stack, key);
			}

			@Override
			public void deleteStack(AppletRef stack) throws NotImplemented,
					TException {
				db.deleteStack(stack);
			}

			@Override
			public void deleteValue(AppletRef stack, String key)
					throws NotImplemented, TException {
				db.deleteValue(stack, key);				
			}

			@Override
			public long getTimeStamp(AppletRef stack) throws NotImplemented,
					TException {
				return db.getTimeStamp(stack);
			}

			@Override
			public List<String> getValueJSON(AppletRef stack, String key)
					throws NotImplemented, TException {
				return db.getValueJSON(stack, key);
			}

			@Override
			public long keyCount(AppletRef stack, int maxCount)
					throws NotImplemented, TException {
				return db.keyCount(stack, maxCount);
			}

			@Override
			public List<String> keySlice(AppletRef stack, String keyStart,
					int count) throws NotImplemented, TException {
				return db.keySlice(stack, keyStart, count);
			}

			@Override
			public List<KeyValJSONPair> keyValJSONSlice(AppletRef stack,
					String startKey, int count) throws NotImplemented,
					TException {
				return db.keyValJSONSlice(stack, startKey, count);
			}

			@Override
			public PeerInfo getPeerInfo() {
				return ((PluginInterface)db).getPeerInfo();
			}

			@Override
			public void start(PluginManager pluginManager) throws Exception {}

			@Override
			public void stop() {}

			@Override
			public List<PeerInfoThrift> peerInfo(List<PeerInfoThrift> info)
					throws NotImplemented, BadPeerName, TException {
				return db.peerInfo(info);
			}
			
			@Override
			public List<String> stackSlice(String startKey, int count)
					throws NotImplemented, TException {
				return db.stackSlice(startKey, count);
			}			
		};		
	}
	
	abstract public void init(Map params) throws ConnectionException;

	public void add( AppletRef stack, long jobID, long emitID, String key, Object object ) throws TException, NotImplemented {
		addDataJSON(stack, jobID, emitID, key, JSON.dumps(object));
	}
	
	
	public List<Object> get(AppletRef stack, String key)
			throws TException, NotImplemented {
		
		List<String> i = getValueJSON(stack, key);

		List<Object> out = new ArrayList<Object>(i.size());
		for (String j : i) {
			out.add(JSON.loads(j));
		}
		return out;
	}
	
	public List<KeyValPair> keyValSlice(AppletRef stack,
			String startKey, int count) throws TException, NotImplemented {
		List<KeyValJSONPair> i = keyValJSONSlice(stack, startKey, count);
		
		List<KeyValPair> out = new ArrayList<KeyValPair>(i.size());
		for (KeyValJSONPair kv : i) {
			out.add(new KeyValPair(kv));
		}
		return out;
	}

	public Iterable<String> listKeys(AppletRef applet) {
		return new RemusDBSliceIterator<String>(this, applet, "", "", false) {
			@Override
			public void processKeyValue(String key, Object val, long jobID, long emitID) {
				addElement(key);
			}			
		};		
	}

	public Iterable<KeyValPair> listKeyPairs(AppletRef applet) {	
		return new RemusDBSliceIterator<KeyValPair>(this, applet, "", "", true) {
			@Override
			public void processKeyValue(String key, Object val, long jobID, long emitID) {
				addElement(new KeyValPair(key, val, jobID, emitID));
			}			
		};		
	}
	
	@Override
	public String status() throws TException {
		return "OK";
	}

	@Override
	public void deleteAttachment(AppletRef stack, String key, String name)
			throws NotImplemented, TException {
		throw new NotImplemented();
	}


	@Override
	public boolean hasAttachment(AppletRef stack, String key, String name)
			throws NotImplemented, TException {
		throw new NotImplemented();
	}


	@Override
	public void initAttachment(AppletRef stack, String key, String name) 
	throws NotImplemented, TException {
		throw new NotImplemented();
	}


	@Override
	public String jobRequest(String dataServer, String attachServer, WorkDesc work)
			throws TException, NotImplemented {
		throw new NotImplemented();
	}


	@Override
	public List<String> listAttachments(AppletRef stack, String key)
			throws NotImplemented, TException {
		throw new NotImplemented();
	}


	@Override
	public ByteBuffer readBlock(AppletRef stack, String key, String name,
			long offset, int length) throws NotImplemented, TException {
		throw new NotImplemented();
	}


	@Override
	public JobStatus jobStatus(String jobID) throws TException, NotImplemented {
		throw new NotImplemented();
	}


	@Override
	public void appendBlock(AppletRef stack, String key, String name, ByteBuffer data) 
	throws NotImplemented, TException {
		throw new NotImplemented();
	}

	@Override
	public int jobCancel(String jobID) throws NotImplemented, TException {
		throw new NotImplemented();	
	}
	
	@Override
	public AttachmentInfo getAttachmentInfo(AppletRef stack, String key,
			String name) throws NotImplemented, TException {
		throw new NotImplemented();	
	}
}
