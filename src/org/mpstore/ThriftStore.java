package org.mpstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
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
import org.apache.thrift.transport.TTransportException;


public class ThriftStore implements MPStore {

	
	
	
	private static final ConsistencyLevel CL = ConsistencyLevel.ONE;

	Serializer serializer;
	String basePath;
	ObjectPool clientPool;

	String columnFamily,keySpace;

	@Override
	public void init(Serializer serializer, String basePath) {
		this.serializer = serializer;
		this.basePath = basePath;
		clientPool = new SoftReferenceObjectPool( new ClientFactory() );
		columnFamily = "remusTable";
		keySpace = "remus";

	}

	class ClientFactory extends BasePoolableObjectFactory {
		@Override
		public Object makeObject() throws Exception {
			try {
				TTransport tr = new TSocket("localhost", 9160);	 //new default in 0.7 is framed transport	 
				TFramedTransport tf = new TFramedTransport(tr);	 
				TProtocol proto = new TBinaryProtocol(tf);	 
				tf.open();
				Client client = new Client(proto);	
				client.set_keyspace(keySpace);
				return client;
			} catch (TTransportException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidRequestException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	 
			return null;
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



	@Override
	public void add(String path, String instance, long jobID, long emitID,
			String key, Object data) {		
		try {
			String column = instance + path;
			ColumnParent cp = new ColumnParent( columnFamily );
			cp.setSuper_column( ByteBuffer.wrap( key.getBytes()) );
			long clock = (new Date()).getTime();
			String colName = Long.toString(jobID) + "_" + Long.toString(emitID);
			Column col = new Column(ByteBuffer.wrap(colName.getBytes()), 
					ByteBuffer.wrap( serializer.dumps(data).getBytes()) , clock);	
			Client client = (Client)clientPool.borrowObject();
			client.insert(ByteBuffer.wrap( column.getBytes() ), cp, col, CL);
			clientPool.returnObject(client);			
		} catch (InvalidRequestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnavailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimedOutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	public boolean containsKey(String path, String instance, String key) {
		try {
			String superColumn = instance + path;
			ColumnPath cp = new ColumnPath( columnFamily );
			cp.setSuper_column( ByteBuffer.wrap(key.getBytes()));
			Client client = (Client)clientPool.borrowObject();
			ColumnOrSuperColumn res = client.get( ByteBuffer.wrap(superColumn.getBytes()), cp, CL);
			if ( res != null )
				return true;
		} catch (InvalidRequestException e) {
			//not found, return false
		} catch (NoSuchElementException e) {
			//not found, return false
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		} catch (NotFoundException e) {
			//not found, return false
		} catch (UnavailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimedOutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public void delete(String path, String instance) {
		try {
			Client client = (Client)clientPool.borrowObject();
			String column = instance + path;
			ColumnPath cp = new ColumnPath( columnFamily );
			long clock = (new Date()).getTime();
			//cp.setColumn( ByteBuffer.wrap( column.getBytes() ) );
			//cp.setSuper_column( ByteBuffer.wrap(column.getBytes()) );
			client.remove( ByteBuffer.wrap( column.getBytes() ), cp, clock, CL);
			clientPool.returnObject(client);
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

	@Override
	public void delete(String path, String instance, String key) {
		try {
			Client client = (Client)clientPool.borrowObject();
			String column = instance + path;
			ColumnPath cp = new ColumnPath( columnFamily );
			long clock = (new Date()).getTime();
			//cp.setColumn( ByteBuffer.wrap( column.getBytes() ) );
			cp.setSuper_column( ByteBuffer.wrap(key.getBytes()) );
			client.remove( ByteBuffer.wrap( column.getBytes() ), cp, clock, CL);
			clientPool.returnObject(client);
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
	
	@Override
	public Iterable<Object> get(String path, String instance, String key) {
		List<Object> out = new LinkedList<Object>();
		try {
			String superColumn = instance + path;
			ColumnPath cp = new ColumnPath( columnFamily );
			cp.setSuper_column( ByteBuffer.wrap(key.getBytes()));			
			Client client = (Client)clientPool.borrowObject();
			ColumnOrSuperColumn res = client.get( ByteBuffer.wrap(superColumn.getBytes()), cp, CL);
			for ( Column col : res.getSuper_column().columns ) {
				out.add( serializer.loads( new String(col.getValue()) ) );
			}
			clientPool.returnObject(client);
		} catch (InvalidRequestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotFoundException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		} catch (UnavailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimedOutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 	
		return out;
	}


	abstract class SliceIterator<T> implements Iterable<T>, Iterator<T> {
		boolean hasMore = true, firstSlice = true, elemAdded;
		int maxFetch = 10;
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
			try {
				SliceRange sRange = new SliceRange(ByteBuffer.wrap(keyStart),ByteBuffer.wrap(keyEnd), false, maxFetch);
				SlicePredicate slice = new SlicePredicate();	 
				slice.setSlice_range(sRange);
				Client client = (Client)clientPool.borrowObject();
				List<ColumnOrSuperColumn> res = client.get_slice( superColumn, cp, slice, CL);
				clientPool.returnObject(client);
				elemAdded = false;
				for ( ColumnOrSuperColumn col : res ) {		
					String curKey = new String( col.getSuper_column().getName() );
					if ( firstSlice || curKey.compareTo( new String(keyStart)) != 0 ) {
						processColumn(col);
						keyStart = col.getSuper_column().getName();						
					}
				}	
				firstSlice = false;
				return elemAdded;				
			} catch (InvalidRequestException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnavailableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TimedOutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
	public InputStream readAttachement(String path, String instance, String key) {
		try {
			String pathStr=null;
			for ( Object obj : get(path, instance, key ) ) {
				pathStr = (String)obj;
			}
			if ( pathStr != null ) {
				InputStream is = new FileInputStream( new File(basePath, pathStr) );
				return is;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void writeAttachment(String path, String instance, String key,
			InputStream inputStream) {
		try {
			MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
			sha1.update( path.getBytes() );
			sha1.update( key.getBytes() );
			byte []hash = sha1.digest();
			Formatter format = new Formatter();
			for ( byte b : hash ) {
				format.format("%02x", b);
			}
			String keyDigest = format.toString();

			File instanceDir = new File( basePath, instance );
			if ( !instanceDir.exists() ) {
				instanceDir.mkdir();
			}
			File outFile = new File( instanceDir, keyDigest );
			FileOutputStream fos = new FileOutputStream(outFile);
			byte [] buffer = new byte[1024];
			int len;
			while ((len=inputStream.read(buffer))>0) {
				fos.write(buffer, 0, len);
			}
			fos.close();
			add( path, instance, 0L, 0L, key, instance + "/" + keyDigest );
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
