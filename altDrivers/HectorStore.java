package org.mpstore;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.OrderedSuperRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;



/*
 * THIS CLASS IS INCOMPLETE AND DOES NOT WORK!!!!!!!!!!!!!!!!
 * 
 */


public class HectorStore implements MPStore {


	Cluster cluster;
	Keyspace keyspaceOperator;
	Mutator<String> mutator;
	String columnFamily;
	private static StringSerializer strSer = StringSerializer.get();

	public HectorStore( String clusterName, String server, String keyspace, String columnFamily ) {
		cluster = HFactory.getOrCreateCluster(clusterName, server );
		keyspaceOperator = HFactory.createKeyspace(keyspace, cluster);
		mutator = HFactory.createMutator(keyspaceOperator, strSer);
		this.columnFamily = columnFamily;
	}

	Serializer serial;	
	@Override
	public void initMPStore(Serializer serializer, Map paramMap) {
		serial = serializer;
	}

	@Override
	public void add(String file, String instance, long jobid, long order, String key, Object data) {
		try {            
			mutator.insert(instance + file, columnFamily, 
					HFactory.createSuperColumn( key, 
							Arrays.asList(HFactory.createStringColumn(Long.toString(jobid) + "_" + Long.toString(order), serial.dumps(data))), 
							strSer, strSer, strSer));
		} catch (HectorException e) {
			e.printStackTrace();
		} 		
	}

	@Override
	public void add(String path, String instance, List<KeyValuePair> inputList) {
		for ( KeyValuePair kv : inputList ) {
			add( path, instance, kv.getJobID(), kv.getEmitID(), kv.getKey(), kv.getValue() );
		}
	}

	@Override
	public boolean containsKey(String file, String instance, String key) {
		List<Object> out = new LinkedList<Object>();		
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
			HFactory.createSuperColumnQuery(keyspaceOperator, strSer, strSer, strSer, strSer);
		superColumnQuery.setColumnFamily(columnFamily).setKey( instance + file ).setSuperName( key );
		QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();
		HSuperColumn<String, String, String> scol = result.get();
		if ( scol != null)
			return true;
		return false;
	}

	@Override
	public Iterable<Object> get(String file, String instance, String key) {
		List<Object> out = new LinkedList<Object>();		
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
			HFactory.createSuperColumnQuery(keyspaceOperator, strSer, strSer, strSer, strSer);
		superColumnQuery.setColumnFamily(columnFamily).setKey( instance + file ).setSuperName( key );
		QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();
		HSuperColumn<String, String, String> scol = result.get();
		if ( scol == null ) {
			return new ArrayList<Object>();
		}
		for ( HColumn<String, String> col : scol.getColumns() ) {
			Object nObj = serial.loads( col.getValue() );
			out.add(nObj);
		}		
		return out;
	}


	abstract class HectorIterator<T> implements Iterable<T>, Iterator<T> {
		LinkedList<T> outList;
		@Override
		public Iterator<T> iterator() {			
			outList = new LinkedList<T>();
			Collection<T> a = prepNextSlice();
			if ( a != null )
				outList.addAll( a );
			return this;
		}

		T nextVal;
		@Override
		public boolean hasNext() {
			if ( outList.size() > 0 )
				return true;
			return false;
		}

		abstract public Collection<T> prepNextSlice();

		@Override
		public T next() {
			if ( outList.size() < 10 ) {
				Collection<T> a = prepNextSlice();
				if ( a != null )
					outList.addAll( a );
			}
			return outList.pollFirst();
		}

		@Override
		public void remove() { }

	}

	@Override
	public Iterable<KeyValuePair> listKeyPairs(String file, String instance) {
		RangeSuperSlicesQuery<String, String, String, String> q = 
			HFactory.createRangeSuperSlicesQuery(keyspaceOperator, strSer, strSer, strSer, strSer);

		String colName =  instance + file ;
		q.setColumnFamily(columnFamily).setKeys(colName, colName);
		q.setRange("", "", false, 10);

		QueryResult<OrderedSuperRows<String, String, String, String>> res = q.execute();
		final OrderedSuperRows<String, String, String, String> slice = res.get();

		HectorIterator<KeyValuePair> out = new HectorIterator<KeyValuePair>() {		
			@Override
			public Collection<KeyValuePair> prepNextSlice() {
				List<KeyValuePair> out = new LinkedList<KeyValuePair>( );
				for ( SuperRow<String, String, String, String> srow : slice ) {
					String key = srow.getKey();
					for ( HSuperColumn<String, String, String> scol : srow.getSuperSlice().getSuperColumns() ) {
						for ( HColumn<String, String> col : scol.getColumns() ) {
							Object object = serial.loads( col.getName() );
							long jobID = 0;
							long emitID = 0;
							String [] tmp = col.getName().split("_");
							if ( tmp.length == 2) {
								jobID = Long.parseLong( tmp[0] );
								emitID = Long.parseLong( tmp[1] );
							}
							out.add(new KeyValuePair(jobID, emitID, key, object));
						}
					}
				}
				return out;
			}		
		};		
		return out;
	}

	@Override
	public Iterable<String> listKeys(String file, String instance) {

		String keySet = instance + file;
		RangeSuperSlicesQuery<String, String, String, String> q = 
			HFactory.createRangeSuperSlicesQuery(keyspaceOperator, strSer, strSer, strSer, strSer);
		q.setColumnFamily(columnFamily);
		q.setKeys(keySet, keySet);
		q.setRange("", "", false, 10);
		//q.setReturnKeysOnly();		
		List<String> out = new ArrayList<String>();	
		QueryResult<OrderedSuperRows<String, String, String, String>> res = q.execute();


		Iterator<SuperRow<String, String, String, String>> i = res.get().iterator();		

		while ( i.hasNext() ) {
			SuperRow<String, String, String, String> r = i.next();
			SuperSlice<String, String, String> s = r.getSuperSlice();			
			for ( HSuperColumn<String, String, String> b : s.getSuperColumns() ) {
				out.add( b.getName() );
			}
		}
		return out;


	}


	@Override
	public void close() {
		cluster.getConnectionManager().shutdown();  		
	}

	@Override
	public void delete(String file, String instance) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(String file, String instance, String key) {
		// TODO Auto-generated method stub

	}


	@Override
	public long keyCount(String path, String instance, int maxCount) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getTimeStamp(String path, String instance) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Iterable<String> keySlice(String path, String instance,
			String startKey, int count) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	@Override
	public Iterable<KeyValuePair> getSlice(String path, String instance,
			String key, int count) {
		// TODO Auto-generated method stub
		return null;
	}
	 */
	



}
