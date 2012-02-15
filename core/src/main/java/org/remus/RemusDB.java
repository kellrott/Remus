package org.remus;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.thrift.TableRef;
import org.remus.thrift.AttachmentInfo;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;

public abstract class RemusDB implements RemusNet.Iface {

	
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
			public void addDataJSON(TableRef stack, String key, String data) throws NotImplemented, TException {
				db.addDataJSON(stack, key, data);
			}

			@Override
			public boolean containsKey(TableRef stack, String key)
					throws NotImplemented, TException {
				return db.containsKey(stack, key);
			}

			@Override
			public void deleteTable(TableRef stack) throws NotImplemented,
					TException {
				db.deleteTable(stack);
			}
			
			@Override
			public List<String> getValueJSON(TableRef stack, String key)
					throws NotImplemented, TException {
				return db.getValueJSON(stack, key);
			}

			@Override
			public long keyCount(TableRef stack, int maxCount)
					throws NotImplemented, TException {
				return db.keyCount(stack, maxCount);
			}

			@Override
			public List<String> keySlice(TableRef stack, String keyStart,
					int count) throws NotImplemented, TException {
				return db.keySlice(stack, keyStart, count);
			}

			@Override
			public List<KeyValJSONPair> keyValJSONSlice(TableRef stack,
					String startKey, int count) throws NotImplemented,
					TException {
				return db.keyValJSONSlice(stack, startKey, count);
			}
			
			@Override
			public List<String> tableSlice(String startKey, int count)
					throws NotImplemented, TException {
				return db.tableSlice(startKey, count);
			}

			@Override
			public void createTable(TableRef table) throws NotImplemented,
					TException {
				db.createTable(table);
			}

			
		};		
	}
	
	abstract public void init(Map params) throws ConnectionException;
	
	public void add( TableRef stack, long jobID, long emitID, String key, Object object ) throws TException, NotImplemented {
		addDataJSON(stack, key, JSON.dumps(object));
	}
	
	
	public List<Object> get(TableRef stack, String key)
			throws TException, NotImplemented {
		
		List<String> i = getValueJSON(stack, key);

		List<Object> out = new ArrayList<Object>(i.size());
		for (String j : i) {
			out.add(JSON.loads(j));
		}
		return out;
	}
	
	public List<KeyValPair> keyValSlice(TableRef stack,
			String startKey, int count) throws TException, NotImplemented {
		List<KeyValJSONPair> i = keyValJSONSlice(stack, startKey, count);
		
		List<KeyValPair> out = new ArrayList<KeyValPair>(i.size());
		for (KeyValJSONPair kv : i) {
			out.add(new KeyValPair(kv));
		}
		return out;
	}

	public Iterable<String> listKeys(TableRef applet) {
		return new RemusDBSliceIterator<String>(this, applet, "", "", false) {
			@Override
			public void processKeyValue(String key, Object val, long jobID, long emitID) {
				addElement(key);
			}			
		};		
	}

	public Iterable<KeyValPair> listKeyPairs(TableRef applet) {	
		return new RemusDBSliceIterator<KeyValPair>(this, applet, "", "", true) {
			@Override
			public void processKeyValue(String key, Object val, long jobID, long emitID) {
				addElement(new KeyValPair(key, val, jobID, emitID));
			}			
		};		
	}
	

	@Override
	public void deleteAttachment(TableRef stack, String key, String name)
			throws NotImplemented, TException {
		throw new NotImplemented();
	}


	@Override
	public boolean hasAttachment(TableRef stack, String key, String name)
			throws NotImplemented, TException {
		throw new NotImplemented();
	}


	@Override
	public void initAttachment(TableRef stack, String key, String name) 
	throws NotImplemented, TException {
		throw new NotImplemented();
	}
	

	@Override
	public List<String> listAttachments(TableRef stack, String key)
			throws NotImplemented, TException {
		throw new NotImplemented();
	}


	@Override
	public ByteBuffer readBlock(TableRef stack, String key, String name,
			long offset, int length) throws NotImplemented, TException {
		throw new NotImplemented();
	}


	@Override
	public void appendBlock(TableRef stack, String key, String name, ByteBuffer data) 
	throws NotImplemented, TException {
		throw new NotImplemented();
	}
	
	@Override
	public AttachmentInfo getAttachmentInfo(TableRef stack, String key,
			String name) throws NotImplemented, TException {
		throw new NotImplemented();	
	}
}
