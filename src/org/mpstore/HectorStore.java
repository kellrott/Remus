package org.mpstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
	public void init(Serializer serializer, String basePath) {
		serial = serializer;
	}

	@Override
	public void add(String file, String instance, long jobid, long order, Object key, Object data) {
		try {            
			mutator.insert(instance + file, columnFamily, 
					HFactory.createSuperColumn(serial.dumps(key), 
							Arrays.asList(HFactory.createStringColumn(Long.toString(jobid) + "_" + Long.toString(order), serial.dumps(data))), 
							strSer, strSer, strSer));
		} catch (HectorException e) {
			e.printStackTrace();
		} 		
	}


	@Override
	public boolean containsKey(String file, String instance, Object key) {
		List<Object> out = new LinkedList<Object>();		
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
			HFactory.createSuperColumnQuery(keyspaceOperator, strSer, strSer, strSer, strSer);
		superColumnQuery.setColumnFamily(columnFamily).setKey( instance + file ).setSuperName( serial.dumps(key) );
		QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();
		HSuperColumn<String, String, String> scol = result.get();
		if ( scol != null)
			return true;
		return false;
	}

	@Override
	public Iterable<Object> get(String file, String instance, Object key) {
		List<Object> out = new LinkedList<Object>();		
		SuperColumnQuery<String, String, String, String> superColumnQuery = 
			HFactory.createSuperColumnQuery(keyspaceOperator, strSer, strSer, strSer, strSer);
		superColumnQuery.setColumnFamily(columnFamily).setKey( instance + file ).setSuperName( serial.dumps(key) );
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

		@Override
		public Iterator<T> iterator() {			
			return this;
		}

		T nextVal;
		@Override
		public boolean hasNext() {
			nextVal = prepNext();
			return false;
		}

		abstract public T prepNext();

		@Override
		public T next() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub

		}

	}

	@Override
	public Iterable<KeyValuePair> listKeyPairs(String file, String instance) {
		SuperSliceQuery<String, String, String, String> q = HFactory.createSuperSliceQuery(keyspaceOperator, strSer, strSer, strSer, strSer);

		q.setColumnFamily(columnFamily).setKey( instance + file );
		q.setRange("", "", false, 10);

		QueryResult<SuperSlice<String, String, String>> res = q.execute();
		final SuperSlice<String, String, String> slice = res.get();

		/*
		HectorIterator<KeyValuePair> out = new HectorIterator<KeyValuePair>() {			
			SuperSlice<String, String, String> a = slice;			
			@Override
			public KeyValuePair prepNext() {
				
			}		
		};
		*/
		
		List<KeyValuePair> out = new LinkedList<KeyValuePair>( );
		List<HSuperColumn<String, String, String>> scols = slice.getSuperColumns();
		for ( HSuperColumn<String, String, String> scol : scols ) {
			Object key = serial.loads( scol.getName() );
			for ( HColumn<String, String> col : scol.getColumns() ) {
				Object object = serial.loads( col.getValue() );
				long jobID = 0;
				long emitID = 0;
				String [] tmp = col.getName().split("_");
				if ( tmp.length == 2) {
					jobID = Long.parseLong( tmp[0] );
					emitID = Long.parseLong( tmp[1] );
				}
				out.add(new KeyValuePair(file, instance, jobID, emitID, key, object));
			}
		}
		return out;
	}

	@Override
	public Iterable<Object> listKeys(String file, String instance) {
		
		String keySet = instance + file;
		RangeSuperSlicesQuery<String, String, String, String> q = 
			HFactory.createRangeSuperSlicesQuery(keyspaceOperator, strSer,strSer,strSer,strSer);
		q.setColumnFamily(columnFamily);
		q.setKeys(keySet, keySet);
		q.setRange("", "", false, 10);
		//q.setReturnKeysOnly();		
		List<Object> out = new ArrayList<Object>();	
		QueryResult<OrderedSuperRows<String, String, String, String>> res = q.execute();
		
		
		Iterator<SuperRow<String, String, String, String>> i = res.get().iterator();		

		while ( i.hasNext() ) {
			SuperRow<String, String, String, String> r = i.next();
			SuperSlice<String, String, String> s = r.getSuperSlice();			
			for ( HSuperColumn<String, String, String> b : s.getSuperColumns() ) {
				out.add( serial.loads( b.getName() ) );
			}
		}
		return out;
		

	}


	@Override
	public void close() {
		cluster.getConnectionManager().shutdown();  		
	}
}
