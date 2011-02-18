package org.mpstore;

import java.util.LinkedList;
import java.util.List;

public class HectorTest {

	static public void main(String []args) {
		
		MPStore ds = new HectorStore("testCluster", "localhost:9160", "remus", "remusTable" );
		//MPStore ds = new SQLStore();
		//MPStore ds = new ThriftStore();
		
		ds.init(new JsonSerializer(), "" );
		
		String instance1 = "00-testing-01";
		String instance2 = "00-testing-02";
		
		String file1 = "@testfile_1";
		String file2 = "@testfile_2";
		String file3 = "@testfile_3";
				
		String keyPath2 ="hello";
		
		String key1 = "key_1";
		String key2 = "key_2";

		String val1 = "value_1";
		String val2 = "value_2";
		
		for ( int i =0; i < 100; i++) {
			ds.add(file1, instance1, 0, i, key1, "value_" + Integer.toString(i) );
		}
		
		for ( int i = 0; i < 100; i++) {
			ds.add(file2, instance2, 0, 0, "key_" + Integer.toString(i), "value" );
		}
		
		List<KeyValuePair> inList = new LinkedList<KeyValuePair>();
		for ( int i = 0; i < 100; i++) {
			inList.add( new KeyValuePair(0, 0, "key_" + Integer.toString(i), "value" ) );
		}
		ds.add(file3 , instance2, inList);
		
		for ( KeyValuePair kv : ds.listKeyPairs(file1, instance1) ) {
			System.out.println( "instance 1 LIST KEYSPAIRS " + kv.getKey() + " " + kv.getValue() );
		}
		
		for ( KeyValuePair kv : ds.listKeyPairs(file2, instance2) ) {
			System.out.println( "instance 2 LIST KEYPAIRS " + kv.getKey() + " " + kv.getValue() );
		}
		
		for ( Object val : ds.get(file1, instance1, key1) ) {
			System.out.println( "instance 1 GET " + val );					
		}
		
		for ( Object key : ds.listKeys(file1, instance1) ) {
			System.out.println( "instance 1 LISTKEY " + key );					
		}
		
		for ( Object key : ds.listKeys(file2, instance2) ) {
			System.out.println( "instance 2 LISTKEY " + key );					
		}
		
		System.out.println( "instance 1 HAS " + ds.containsKey(file1, instance1, key1) );
		System.out.println( "instance 1 HAS " + ds.containsKey(file1, instance1, key2) );
		
			
		ds.delete( file1, instance1, key1 );
		ds.delete( file2, instance2 );

		for ( int i = 0; i < 50; i++ ) {
			ds.delete(file3, instance2, "key_" + Integer.toString(i) );
		}

		
		ds.close();
		
		System.out.println( "instance 1 HAS " + ds.containsKey(file1, instance1, key1) );
		System.out.println( "instance 1 HAS " + ds.containsKey(file1, instance1, key2) );
	}
	
}
