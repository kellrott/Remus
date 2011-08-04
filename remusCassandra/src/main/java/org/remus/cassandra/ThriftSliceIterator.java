package org.remus.cassandra;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;


abstract class ThriftSliceIterator<T> implements Iterable<T>, Iterator<T> {
	private static final ConsistencyLevel CL = ConsistencyLevel.ONE;

	boolean hasMore = true, firstSlice = true, elemAdded;
	int maxFetch = 100;
	Integer maxCount = null;
	LinkedList<T> outList;
	byte [] keyStart,  keyEnd;
	ColumnParent cp;
	ByteBuffer superColumn;

	ThriftClientPool clientPool;

	public ThriftSliceIterator(ThriftClientPool clientPool, String superColumn, String columnParent, String keyStart, String keyEnd) {
		this.clientPool = clientPool;
		this.keyStart = keyStart.getBytes();
		this.keyEnd = keyEnd.getBytes();
		this.superColumn = ByteBuffer.wrap( superColumn.getBytes() );
		cp = new ColumnParent(columnParent);
		outList = new LinkedList<T>();
	}

	public ThriftSliceIterator(ThriftClientPool clientPool, String superColumn, String columnParent, String keyStart, String keyEnd, int maxCount) {
		this.clientPool = clientPool;
		this.keyStart = keyStart.getBytes();
		this.keyEnd = keyEnd.getBytes();
		this.superColumn = ByteBuffer.wrap( superColumn.getBytes() );
		cp = new ColumnParent(columnParent);
		outList = new LinkedList<T>();
		this.maxCount = maxCount;
	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {		
		if ( hasMore && outList.size() < maxFetch ) {
			hasMore = getNextSlice();
		}
		if ( maxCount != null && maxCount <= 0 ) 
			return false;
		if ( outList.size() <= 0 )
			return false;
		return true;
	}

	private boolean getNextSlice() {
		ThriftCaller<Boolean> getSliceCall = new ThriftCaller<Boolean>(clientPool) {
			@Override
			protected Boolean request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {				
				SliceRange sRange = new SliceRange(ByteBuffer.wrap(keyStart),ByteBuffer.wrap(keyEnd), false, maxFetch);
				SlicePredicate slice = new SlicePredicate();	 
				slice.setSlice_range(sRange);
				List<ColumnOrSuperColumn> res = client.get_slice( superColumn, cp, slice, CL);
				for ( ColumnOrSuperColumn col : res ) {		
					String curKey = new String( col.getSuper_column().getName() );
					if ( firstSlice || curKey.compareTo( new String(keyStart)) != 0 ) {
						processColumn(col);
						keyStart = col.getSuper_column().getName();
						elemAdded = true;
					}
				}	
				firstSlice = false;
				return elemAdded;
			}
		};

		try {
			return getSliceCall.call();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	abstract void processColumn( ColumnOrSuperColumn col );

	void addElement( T elem ) {
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