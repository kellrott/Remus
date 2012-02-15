package org.remus.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

//import junit
import org.apache.thrift.TException;
import org.junit.*;

import org.remus.ConnectionException;
import org.remus.KeyValPair;
import org.remus.RemusDB;
import org.remus.RemusDBSliceIterator;
import org.remus.thrift.TableRef;
import org.remus.thrift.NotImplemented;

public class MPStoreTest {

	RemusDB ds;

	String instance1 = "00-testing-01";
	String instance2 = "00-testing-02";

	String applet1 = "@testfile_1";
	String applet2 = "@testfile_2";

	@Before public void setUp() throws FileNotFoundException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, ConnectionException {
		String CLASS_NAME = "org.remus.cassandra.Server";
		Class<RemusDB> cls = (Class<RemusDB>) Class.forName( CLASS_NAME );
		
		Map config = new HashMap();
		config.put("columnFamily", "remusTest");
		config.put("keySpace", "remus");		
		
		ds = cls.newInstance();
		ds.init(config);		
	}

	@Test public void insertTest() throws TException, NotImplemented {

		
		TableRef aRef1 = new TableRef(instance1, applet1);
		TableRef aRef2 = new TableRef(instance2, applet2);
		
		String keyPath2 ="hello";

		String key1 = "key_1";
		String key2 = "key_2";

		String val1 = "value_1";
		String val2 = "value_2";

		for ( long i =0; i < 100; i++) {
			ds.add( aRef1, 0L, i, key1, "value_" + Long.toString(i) );
		}

		for ( int i = 0; i < 100; i++) {
			ds.add( aRef2, 0L, 0L, "key_" + Integer.toString(i), "value_" + i );
		}

		int count = 0;

		count=10;
		for ( String key : ds.keySlice(aRef2, "key_10", 10 ) ) {
			Assert.assertTrue( key.compareTo("key_"+Integer.toString(count) ) == 0 );
			count++;
		}
		
		count = 0;
		for ( KeyValPair kv : ds.keyValSlice(aRef1, "", 200) ) {
			Object value = kv.getValue();
			Assert.assertTrue( ((String)value).compareTo( "value_" + kv.getEmitID() ) == 0);
			count++;
		}
		Assert.assertTrue( count == 100 );
		
		//assert the correct number of keys exist
		count = 0;
		for ( KeyValPair kv : ds.keyValSlice(aRef2, "", 200) ) {
			count++;
			System.err.println( kv.getValue() );
		}
		Assert.assertTrue( count == 100 );

		count = 0;
		for ( Object val : ds.get(aRef1, key1) ) {
			count++;
			Assert.assertTrue( ((String)val).startsWith("value_") );
		}
		Assert.assertTrue( count == 100 );

		//assert that keys are listed in a sorted order
		String lastKey = null;
		for ( String key : ds.keySlice(aRef2, "", 1000) ) {
			if ( lastKey != null ) {
				Assert.assertTrue( key.compareTo(lastKey) >= 1  );
				Assert.assertTrue( lastKey.compareTo(key) <= -1  );
			}
			lastKey = key;
		}

		//for ( Object key : ds.listKeys(file2, instance2) ) {
		//	System.out.println( "instance 2 LISTKEY " + key );					
		//}
		
		Assert.assertTrue( ds.containsKey(aRef1, key1) );
		Assert.assertTrue( !ds.containsKey(aRef1, key2) );

	}

	
	private static final int CYCLE_1 = 2000;
	private static final int CYCLE_2 = 100;
	
	@Test public void slicerTest() throws TException, NotImplemented {
		TableRef aRef1 = new TableRef(instance1, applet1);
		String key = "key_";
		for (long i = 0; i < CYCLE_1; i++) {
			for (long j = 0; j < CYCLE_2; j++) {
				ds.add(aRef1, i, j, key + Long.toString(i), "value_" + Long.toString(i) + "_" + Long.toString(j));
			}
		}
	
		RemusDBSliceIterator<Object []> out = new RemusDBSliceIterator<Object []>(ds, aRef1, "", "", true) {			
			@Override
			public void processKeyValue(String key, Object val, long jobID, long emitID) {
				addElement(new Object []  {key, val});
			}
		};
		
		int count = 0;
		for (Object [] pair : out) {
			System.out.println(pair[0] + " " + pair[1]);
			count++;
		}		
		Assert.assertEquals(count, CYCLE_1 * CYCLE_2);
		ds.deleteTable(aRef1);
	}
	
	@After public void shutdown() throws NotImplemented, TException {
		TableRef aRef1 = new TableRef(instance1, applet1);
		ds.deleteTable(aRef1);
	}
}
