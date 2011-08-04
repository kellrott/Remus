package org.remus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.plugin.PluginInterface;
import org.remus.thrift.AppletRef;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.RemusDBThrift.Iface;

public abstract class RemusDB implements Iface, PluginInterface {

	abstract public void init(Map params) throws ConnectionException;

	public void add( AppletRef stack, long jobID, long emitID, String key, Object object ) throws TException {
		addData(stack, jobID,emitID, key, JSON.dumps(object));
	}
	
	
	public List<Object> get(AppletRef stack, String key)
			throws TException {
		
		List<String> i = getValueJSON(stack, key);

		List<Object> out = new ArrayList<Object>(i.size());
		for ( String j : i ) {
			out.add(JSON.loads(j));
		}
		return out;
	}
	
	public List<KeyValPair> keyValSlice(AppletRef stack,
			String startKey, int count) throws TException {
		List<KeyValJSONPair> i = keyValJSONSlice(stack, startKey, count);
		
		List<KeyValPair> out = new ArrayList<KeyValPair>( i.size() );
		for ( KeyValJSONPair kv : i ) {
			out.add( new KeyValPair(kv) );
		}
		return out;
	}

	public Iterable<String> listKeys(AppletRef applet) {
		return new RemusDBSliceIterator<String>(this, applet, "", "", false) {
			@Override
			void processKeyValue(String key, Object val, long jobID, long emitID) {
				addElement(key);
			}			
		};		
	}

	public Iterable<KeyValPair> listKeyPairs(AppletRef applet) {	
		return new RemusDBSliceIterator<KeyValPair>(this, applet, "", "", true) {
			@Override
			void processKeyValue(String key, Object val, long jobID, long emitID) {
				addElement(new KeyValPair(key, val, jobID, emitID));
			}			
		};		
	}
}
