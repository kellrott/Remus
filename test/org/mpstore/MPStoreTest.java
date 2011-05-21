package org.mpstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.*;
import org.remus.RemusApp;
import org.remus.RemusDatabaseException;

public class MPStoreTest {

	MPStore ds;


	@Before public void setUp() throws FileNotFoundException, IOException, MPStoreConnectException {
		Properties prop = new Properties();
		prop.load( new FileInputStream( new File( "cassandra.prop" ) ) );
		ds = new ThriftStore();//"testCluster", "localhost:9160", "remus", "remusTable" );
		ds.initMPStore(new JsonSerializer(), prop );
	}

	@Test public void insertTest() {
		String instance1 = "00-testing-01";
		String instance2 = "00-testing-02";
		String instance3 = "00-testing-02";

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

		int count = 0;

		count=10;
		for ( String key : ds.keySlice(file2, instance2, "key_10", 10 ) ) {
			Assert.assertTrue( key.compareTo("key_"+Integer.toString(count) ) == 0 );
			count++;
		}
		
		List<KeyValuePair> inList = new LinkedList<KeyValuePair>();
		for ( int i = 0; i < 100; i++) {
			inList.add( new KeyValuePair(0, 0, "key_" + Integer.toString(i), "value" ) );
		}
		ds.add(file3 , instance3, inList);

		count = 0;
		for ( KeyValuePair kv : ds.listKeyPairs(file1, instance1) ) {
			Assert.assertTrue( ((String)kv.getValue()).compareTo( "value_" + kv.getEmitID() ) == 0);
			count++;
		}
		Assert.assertTrue( count == 100 );
		
		//assert the correct number of keys exist
		count = 0;
		for ( KeyValuePair kv : ds.listKeyPairs(file2, instance2) ) {
			count++;
		}
		Assert.assertTrue( count == 100 );

		count = 0;
		for ( Object val : ds.get(file1, instance1, key1) ) {
			count++;
			Assert.assertTrue( ((String)val).startsWith("value_") );
		}
		Assert.assertTrue( count == 100 );

		//assert that keys are listed in a sorted order
		String lastKey = null;
		for ( String key : ds.listKeys(file2, instance2) ) {
			if ( lastKey != null ) {
				Assert.assertTrue( key.compareTo(lastKey) >= 1  );
				Assert.assertTrue( lastKey.compareTo(key) <= -1  );
			}
			lastKey = key;
		}

		//for ( Object key : ds.listKeys(file2, instance2) ) {
		//	System.out.println( "instance 2 LISTKEY " + key );					
		//}
		
		Assert.assertTrue( ds.containsKey(file1, instance1, key1) );
		Assert.assertTrue( !ds.containsKey(file1, instance1, key2) );


		Date curDate = new Date();
		//Assert that the timestamp is within the last 10 seconds
		Assert.assertTrue( (curDate.getTime() - (ds.getTimeStamp(file1, instance1)/1000)) < 10000   );

		ds.delete( file1, instance1, key1 );
		
		Assert.assertTrue( !ds.containsKey(file1, instance1, key1)  );
		
		ds.delete( file2, instance2 );

		//Delete half the keys and make sure 50 are left
		for ( int i = 0; i < 50; i++ ) {
			ds.delete(file3, instance3, "key_" + Integer.toString(i) );
		}
		count = 0;
		for ( String key : ds.listKeys(file3, instance3)) {
			count++;
		}
		Assert.assertTrue( count == 50 );
		//delete the rest of the keys
		for ( int i = 50; i < 100; i++ ) {
			ds.delete(file3, instance3, "key_" + Integer.toString(i) );
		}
		count = 0;
		for ( String key : ds.listKeys(file3, instance3)) {
			count++;
		}
		Assert.assertTrue( count == 0 );
		
		//assert that keys have been deleted
		Assert.assertTrue( !ds.containsKey(file1, instance1, key1) );
		Assert.assertTrue( !ds.containsKey(file1, instance1, key2) );
	}

	
	@After public void shutdown() {
		ds.close();
	}
}
