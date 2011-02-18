package org.mpstore;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Test {

	public static void main( String [] args ) {
		//System.setProperty("me.prettyprint.hector.TimingLogger", "none" );
		//MPStore dataStore = new HectorStore("testcluster", "localhost:9160", "remus", "remusTable" );
		MPStore dataStore = new ThriftStore();
		
		dataStore.init( new JsonSerializer() , "" );
		
		String instance1 = UUID.randomUUID().toString();
		
		Map<String,String> testTable = new HashMap<String,String>( );
		
		
		for ( int i = 0; i < 2; i++ ) {
			String key  = UUID.randomUUID().toString();
			String data = UUID.randomUUID().toString();	
			testTable.put(key, data);
			dataStore.add("/@data", instance1, 0, 0, key, data);
		}
		
		for ( String key : testTable.keySet() ) {
			if ( dataStore.containsKey("/@data", instance1, key)  ) {
				System.out.println( "Found" );
			}
			int count = 0;			
			for ( Object val : dataStore.get("/@data", instance1, key ) ) {
				assert( ((String)val).compareTo(testTable.get(key)) == 0 );
				System.out.println( "Match:" + val + "  " + testTable.get(key) );
				count++;
			}
			System.out.println( "Hit count:" + count );
			assert( count == 1);
		}
		
		
		
		for ( Object key : dataStore.listKeys("/@data", instance1) ) {
			System.out.println( key );
		}
		
		
		dataStore.close();
		
	}
	
	
}
