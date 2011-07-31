package org.remusNet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

//import junit
import org.apache.thrift.TException;
import org.junit.*;

import org.remusNet.RemusDB;
import org.remusNet.thrift.AppletRef;
import org.remusNet.thrift.KeyValPair;

public class MPStoreTest {

	RemusDB ds;


	@Before public void setUp() throws FileNotFoundException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, ConnectionException {
		Properties prop = new Properties();
		prop.load( new FileInputStream( new File( "cassandra.prop" ) ) );
		
		Class<RemusDB> cls = (Class<RemusDB>) Class.forName( prop.getProperty( "org.remus.DB_DRIVER" ) );
		
		ds = cls.newInstance();
		ds.init(prop);
		
		//ds = new ThriftStore();//"testCluster", "localhost:9160", "remus", "remusTable" );
		//ds.initMPStore(new JsonSerializer(), prop );
	}

	@Test public void insertTest() throws TException {

		String instance1 = "00-testing-01";
		String instance2 = "00-testing-02";
		String instance3 = "00-testing-02";

		String applet1 = "@testfile_1";
		String applet2 = "@testfile_2";
		String applet3 = "@testfile_3";

		AppletRef aRef1 = new AppletRef("unitTest", instance1, applet1);
		AppletRef aRef2 = new AppletRef("unitTest", instance2, applet2);
		AppletRef aRef3 = new AppletRef("unitTest", instance3, applet3);
		
		String keyPath2 ="hello";

		String key1 = "key_1";
		String key2 = "key_2";

		String val1 = "value_1";
		String val2 = "value_2";

		for ( long i =0; i < 100; i++) {
			ds.addData( aRef1, 0L, i, key1, "value_" + Long.toString(i) );
		}

		for ( int i = 0; i < 100; i++) {
			ds.addData( aRef2, 0L, 0L, "key_" + Integer.toString(i), "value" );
		}

		int count = 0;

		count=10;
		for ( String key : ds.keySlice(aRef2, "key_10", 10 ) ) {
			Assert.assertTrue( key.compareTo("key_"+Integer.toString(count) ) == 0 );
			count++;
		}
		
		count = 0;
		for ( KeyValPair kv : ds.keyValSlice(aRef1, "", 200) ) {
			Assert.assertTrue( ((String)kv.getValue()).compareTo( "value_" + kv.getEmitID() ) == 0);
			count++;
		}
		Assert.assertTrue( count == 100 );
		
		//assert the correct number of keys exist
		count = 0;
		for ( KeyValPair kv : ds.keyValSlice(aRef2, "", 200) ) {
			count++;
		}
		Assert.assertTrue( count == 100 );

		count = 0;
		for ( Object val : ds.getValue(aRef1, key1) ) {
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

		Date curDate = new Date();
		//Assert that the timestamp is within the last 10 seconds
		Assert.assertTrue( (curDate.getTime() - (ds.getTimeStamp(aRef1)/1000)) < 10000   );

		ds.deleteValue(aRef1, key1 );
		
		Assert.assertTrue( !ds.containsKey(aRef1, key1)  );
		
		//Delete half the keys and make sure 50 are left
		for ( int i = 0; i < 50; i++ ) {
			ds.deleteValue(aRef2, "key_" + Integer.toString(i) );
		}
		count = 0;
		for ( String key : ds.keySlice(aRef2, "", 100)) {
			count++;
		}
		Assert.assertTrue( count == 50 );
		//delete the rest of the keys
		for ( int i = 50; i < 100; i++ ) {
			ds.deleteValue( aRef2, "key_" + Integer.toString(i) );
		}
		count = 0;
		for ( String key : ds.keySlice(aRef2, "", 200)) {
			count++;
		}
		Assert.assertTrue( count == 0 );
		
		//assert that keys have been deleted
		Assert.assertTrue( !ds.containsKey(aRef1, key1) );
		Assert.assertTrue( !ds.containsKey(aRef1, key2) );
	}

	
	@After public void shutdown() {

	}
}
