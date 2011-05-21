package org.mpstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.SoftReferenceObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.remus.RemusApp;


public class ThriftStore implements MPStore {
	private static final ConsistencyLevel CL = ConsistencyLevel.ONE;

	Serializer serializer;
	String basePath;
	ObjectPool clientPool;

	String columnFamily,keySpace,serverName;
	int serverPort;

	public static final String COLUMN_FAMILY = "org.mpstore.ThriftStore.columnFamily";
	public static final String KEY_SPACE = "org.mpstore.ThriftStore.keySpace";
	public static final String SERVER = "org.mpstore.ThriftStore.server";
	public static final String PORT = "org.mpstore.ThriftStore.port";

	@Override
	public void initMPStore(Serializer serializer, Map paramMap) throws MPStoreConnectException {
		this.serializer = serializer;
		this.basePath = (String)paramMap.get(RemusApp.configWork);
		clientPool = new SoftReferenceObjectPool( new ClientFactory() );
		columnFamily = (String)paramMap.get(COLUMN_FAMILY);
		keySpace     = (String)paramMap.get(KEY_SPACE);
		serverName = "localhost";
		if ( paramMap.containsKey(SERVER))
			serverName   = (String)paramMap.get(SERVER);
		serverPort = 9160;
		if ( paramMap.containsKey(PORT) )
			serverPort   = Integer.parseInt((String)paramMap.get(PORT));

		try {
			//Check db schema		
			TTransport tr = new TSocket(serverName, serverPort);	 //new default in 0.7 is framed transport	 
			TFramedTransport tf = new TFramedTransport(tr);	 
			TProtocol proto = new TBinaryProtocol(tf);	 
			tf.open();
			Client client = new Client(proto);
			KsDef ksDesc = null;
			try {
				ksDesc = client.describe_keyspace(keySpace);				
			} catch ( NotFoundException e ) {
				String strategy = "org.apache.cassandra.locator.SimpleStrategy";				
				ksDesc = new KsDef(keySpace, strategy, 1, new ArrayList<CfDef>() );				
				try {
					client.system_add_keyspace( ksDesc  );
				} catch ( Exception e2 ) {
					throw new MPStoreConnectException( "Unable to connect or create keyspace " + keySpace + "\n" + e2.toString() );
				}
				//throw new MPStoreConnectException( "Keyspace " + keySpace + " not found" );				
			}			
			
			Boolean found = false;
			for ( CfDef cfdef : ksDesc.getCf_defs() ) {
				if ( cfdef.name.compareTo( columnFamily ) == 0 ) {
					found = true;
				}
			}
			if ( !found ) {				
				CfDef cfDesc = new CfDef(keySpace, columnFamily);
				cfDesc.comparator_type =  "UTF8Type";
				cfDesc.column_type = "Super";
				//ksDesc.addToCf_defs(cfDesc);
				try { 
					client.set_keyspace(keySpace);
					client.system_add_column_family(cfDesc);
				} catch ( Exception e2 ) {
					throw new MPStoreConnectException( "Unable to find or create columnFamily " + columnFamily + "\n" + e2.toString() );
				}
			}
			tf.close();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	class ClientFactory extends BasePoolableObjectFactory {
		@Override
		public Object makeObject() throws Exception {
			TTransport tr = new TSocket(serverName, serverPort);	 //new default in 0.7 is framed transport	 
			TFramedTransport tf = new TFramedTransport(tr);	 
			TProtocol proto = new TBinaryProtocol(tf);	 
			tf.open();
			Client client = new Client(proto);	
			client.set_keyspace(keySpace);
			return client;
		}

		@Override
		public void destroyObject(Object obj) throws Exception {
			((Client)obj).getInputProtocol().getTransport().close();
		}
		@Override
		public boolean validateObject(Object obj) {
			return ((Client)obj).getInputProtocol().getTransport().isOpen();
		}
	}


	abstract class ThriftCaller<T> {
		public T call() throws Exception {
			T out = null;
			boolean done = false;
			int retryCount = 3;
			Exception outE = null;
			do {
				outE = null;
				Client client = null;
				try {
					client = (Client)clientPool.borrowObject();
					out = request( client );
					done = true;
				} catch (InvalidRequestException e) {	
					outE = e;
				} catch (UnavailableException e) {
					outE = e;
					try {
						clientPool.invalidateObject(client);
					} catch (Exception e2) {						
					}
					client = null;					
					try {
						Thread.sleep(4000);
					} catch (InterruptedException e1) {						
					}				
				} catch (TimedOutException e) {
					outE = e;
					try {
						Thread.sleep(4000);
					} catch (InterruptedException e1) {						
					}
				} catch (TException e) {					
					outE = e;
				} catch (NoSuchElementException e) {
					outE = e;
					done = true;
				} catch (IllegalStateException e) {
					outE = e;
				} catch (Exception e) {
					outE = e;
					retryCount=0;
				} finally {
					try {
						if ( client != null )
							clientPool.returnObject(client);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				retryCount--;
			} while ( retryCount > 0 && !done );		
			if ( outE != null ) {
				throw outE;
			}
			return out;
		}		
		protected abstract T request( Client client ) throws InvalidRequestException, UnavailableException, TimedOutException, TException ;
	}



	@Override
	public void add(String path, String instance, long jobID, long emitID,
			String key, Object data) {		

		final String column = instance + path;
		final ByteBuffer superColumn = ByteBuffer.wrap( key.getBytes());
		final String colName = Long.toString(jobID) + "_" + Long.toString(emitID);
		final ByteBuffer colData = ByteBuffer.wrap( serializer.dumps(data).getBytes());

		ThriftCaller<Boolean> addCall = new ThriftCaller<Boolean>()  {
			@Override
			protected Boolean request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {
				ColumnParent cp = new ColumnParent( columnFamily );
				cp.setSuper_column( superColumn );
				long clock = System.currentTimeMillis() * 1000;				
				Column col = new Column(ByteBuffer.wrap(colName.getBytes()), 
						colData , clock);	
				client.insert(ByteBuffer.wrap( column.getBytes() ), cp, col, CL);
				return true;
			}			
		};
		try {
			addCall.call();
		} catch (Exception e) {
			// TODO Auto-generated catch block
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
	public void close() {
		try {
			clientPool.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	@Override
	public boolean containsKey(String path, String instance, String key)  {		
		final String superColumn = instance + path;
		final ColumnPath cp = new ColumnPath( columnFamily );
		cp.setSuper_column( ByteBuffer.wrap(key.getBytes()));

		ThriftCaller<Boolean> containsCall = new ThriftCaller<Boolean>() {
			@Override
			protected Boolean request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {

				boolean returnVal = false;
				try {
					ColumnOrSuperColumn res = client.get( ByteBuffer.wrap(superColumn.getBytes()), cp, CL);
					if ( res != null )
						returnVal = true;
				} catch (NoSuchElementException e) {					
				} catch (NotFoundException e) {					
				}
				return returnVal;
			}			
		};		
		try {
			return containsCall.call();
		} catch (Exception e) {}
		return false;
	}

	@Override
	public void delete(String path, String instance) {

		final String column = instance + path;
		final ColumnPath cp = new ColumnPath( columnFamily );

		ThriftCaller<Boolean> delCall = new ThriftCaller<Boolean>() {
			@Override
			protected Boolean request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {

				long clock = System.currentTimeMillis() * 1000;
				client.remove( ByteBuffer.wrap( column.getBytes() ), cp, clock, CL);

				return null;
			}

		};
		try {
			delCall.call();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void delete(String path, String instance, String key) {

		final String column = instance + path;
		final ColumnPath cp = new ColumnPath( columnFamily );
		cp.setSuper_column( ByteBuffer.wrap(key.getBytes()) );

		ThriftCaller<Boolean> delCall = new ThriftCaller<Boolean>() {
			@Override
			protected Boolean request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {

				long clock = System.currentTimeMillis() * 1000;
				client.remove( ByteBuffer.wrap( column.getBytes() ), cp, clock, CL);
				return null;
			}
		};
		try {
			delCall.call();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Iterable<Object> get(String path, String instance, String key) {

		final String superColumn = instance + path;
		final ColumnPath cp = new ColumnPath( columnFamily );
		cp.setSuper_column( ByteBuffer.wrap(key.getBytes()));			
		ThriftCaller<Iterable<Object>> getCall = new ThriftCaller<Iterable<Object>>() {

			@Override
			protected Iterable<Object> request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {
				List<Object> out = new LinkedList<Object>();
				ColumnOrSuperColumn res;
				try {
					res = client.get( ByteBuffer.wrap(superColumn.getBytes()), cp, CL);
					for ( Column col : res.getSuper_column().columns ) {
						out.add( serializer.loads( new String(col.getValue()) ) );
					}
				} catch (NotFoundException e) {					
				}
				return out;
			}
		};

		try {
			return getCall.call();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}


	abstract class SliceIterator<T> implements Iterable<T>, Iterator<T> {
		boolean hasMore = true, firstSlice = true, elemAdded;
		int maxFetch = 100;
		LinkedList<T> outList;
		byte [] keyStart,  keyEnd;
		ColumnParent cp;
		ByteBuffer superColumn;


		public SliceIterator(String superColumn, String columnParent, String keyStart, String keyEnd) {
			this.keyStart = keyStart.getBytes();
			this.keyEnd = keyEnd.getBytes();
			this.superColumn = ByteBuffer.wrap( superColumn.getBytes() );
			cp = new ColumnParent(columnParent);
			outList = new LinkedList<T>();
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
			if ( outList.size() <= 0 )
				return false;
			return true;
		}

		private boolean getNextSlice() {
			ThriftCaller<Boolean> getSliceCall = new ThriftCaller<Boolean>() {
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
			return outList.removeFirst();
		}

		@Override
		public void remove() { }
	}

	@Override
	public Iterable<KeyValuePair> listKeyPairs(String path, String instance) {
		String superColumn = instance + path;
		SliceIterator<KeyValuePair> out = new SliceIterator<KeyValuePair>(superColumn, columnFamily, "","") {				
			@Override
			void processColumn(ColumnOrSuperColumn scol) {
				String key = new String( scol.getSuper_column().getName() );
				for ( Column col : scol.getSuper_column().getColumns() ) {
					String itemName=new String(col.getName());
					String [] tmp = itemName.split("_");
					long jobID = Long.parseLong(  tmp[0] );
					long workID = Long.parseLong( tmp[1] );
					Object value = serializer.loads( new String(col.getValue()) );
					addElement( new KeyValuePair(jobID, workID, key, value) );
				}
			}
		};
		return out;
	}

	@Override
	public Iterable<String> listKeys(String path, String instance) {
		String superColumn = instance + path;
		SliceIterator<String> out = new SliceIterator<String>(superColumn, columnFamily, "","") {				
			@Override
			void processColumn(ColumnOrSuperColumn col) {
				addElement( new String(col.getSuper_column().getName()) );
			}
		};
		return out;
	}


	@Override
	public long keyCount(String path, String instance, final int maxCount) {
		final String superColumn = instance + path;
		final ColumnParent cp = new ColumnParent(columnFamily);

		ThriftCaller<Long> getKeyCount = new ThriftCaller<Long>() {
			@Override
			protected Long request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {
				SliceRange sRange = new SliceRange(ByteBuffer.wrap("".getBytes()),ByteBuffer.wrap("".getBytes()), false, maxCount);
				SlicePredicate slice = new SlicePredicate();	 
				slice.setSlice_range(sRange);
				long count = client.get_count( ByteBuffer.wrap(superColumn.getBytes()), cp, slice, CL);

				return count;
			}
		};

		try {
			return getKeyCount.call();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return  0;
	}

	@Override
	public long getTimeStamp(String path, String instance) {
		String superColumn = instance + path;
		SliceIterator<Long> out = new SliceIterator<Long>(superColumn, columnFamily, "","") {				
			@Override
			void processColumn(ColumnOrSuperColumn scol) {
				for ( Column col : scol.getSuper_column().getColumns() ) {
					addElement( col.timestamp );
				}
			}
		};

		long timestamp = 0;
		while ( out.hasNext() ) {
			long cur = out.next();
			if ( cur > timestamp )
				timestamp = cur;
		}
		return timestamp;
	}

	@Override
	public Iterable<String> keySlice(String path, String instance, final String startKey, final int count) {
		final String superColumn = instance + path;
		ThriftCaller<Iterable<String>> keySliceCall = new ThriftCaller<Iterable<String>>() {

			@Override
			protected Iterable<String> request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {
				List<String> out = new ArrayList<String>();
				SliceRange sRange = new SliceRange(ByteBuffer.wrap(startKey.getBytes()),ByteBuffer.wrap("".getBytes()), false, count);
				SlicePredicate slice = new SlicePredicate();	 
				slice.setSlice_range(sRange);
				ColumnParent cp = new ColumnParent(columnFamily);
				List<ColumnOrSuperColumn> res = client.get_slice( ByteBuffer.wrap( superColumn.getBytes() ), cp, slice, CL);
				for ( ColumnOrSuperColumn col : res ) {		
					String curKey = new String( col.getSuper_column().getName() );
					out.add(curKey);
				}	
				return out;
			}

		};
		try {
			return keySliceCall.call();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
