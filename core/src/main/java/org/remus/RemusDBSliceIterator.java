package org.remus;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.thrift.TException;
import org.remus.thrift.AppletRef;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;


public abstract class RemusDBSliceIterator<T> implements Iterable<T>, Iterator<T> {
	boolean hasMore = true, firstSlice = true, elemAdded;
	int maxFetch = 100;
	Integer maxCount = null;
	LinkedList<T> outList;
	String keyStart,  keyEnd;
	ByteBuffer superColumn;

	RemusDB db;
	private AppletRef stack;
	boolean loadVal;
	public RemusDBSliceIterator(RemusDB db, AppletRef stack, String keyStart, String keyEnd, boolean loadVal) {
		this.db = db;
		this.stack = stack;
		this.keyStart = keyStart;
		this.loadVal = loadVal;
		this.keyEnd = keyEnd;
		outList = new LinkedList<T>();
	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {		
		if (hasMore && outList.size() < maxFetch) {
			hasMore = getNextSlice();
		}
		if (maxCount != null && maxCount <= 0) {
			return false;
		}
		if (outList.size() <= 0) {
			return false;
		}
		return true;
	}

	private boolean getNextSlice() {
		try {
			String lastKey = null;
			elemAdded = false;
			if (loadVal) {
				List<KeyValJSONPair> curlist = db.keyValJSONSlice(stack, keyStart, maxFetch);
				for ( KeyValJSONPair kv : curlist ){
					if ( firstSlice || kv.key.compareTo( new String(keyStart)) != 0 ) {
						processKeyValue( kv.key, JSON.loads(kv.valueJson), kv.jobID, kv.emitID );
						lastKey = kv.key;
						elemAdded = true;
					}
				}
			} else {
				List<String> curlist = db.keySlice(stack, keyStart, maxFetch);
				for ( String key : curlist ){
					if ( firstSlice || key.compareTo( new String(keyStart)) != 0 ) {
						processKeyValue( key, null, 0, 0 );
						lastKey = key;
						elemAdded = true;
					}
				}
			}
			keyStart = lastKey;
			firstSlice = false;
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
		return elemAdded;
	}

	public abstract void processKeyValue(String key, Object val, long jobID, long emitID);

	public void addElement( T elem ) {
		elemAdded = true;
		outList.add( elem );
	}
	@Override
	public T next() {
		if ( maxCount != null )
			maxCount--;
		return outList.removeFirst();
	}

	@Override
	public void remove() { }
}

