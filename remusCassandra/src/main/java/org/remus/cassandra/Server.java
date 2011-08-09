package org.remus.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.remus.ConnectionException;
import org.remus.PeerInfo;
import org.remus.RemusDB;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerType;
import org.remus.thrift.WorkDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Server extends RemusDB {
	private static final ConsistencyLevel CL = ConsistencyLevel.ONE;

	ThriftClientPool clientPool;
	String columnFamily,keySpace,serverName;
	int serverPort;
	boolean instanceColumns;
	Map<String,String> columns = new HashMap<String,String>();

	private Logger logger;

	public static final String COLUMN_FAMILY = "columnFamily";
	public static final String KEY_SPACE = "keySpace";
	public static final String SERVER = "server";
	public static final String PORT = "port";
	public static final String INST_COLUMNS = "instColumns";
	public static final int CASSANDRA_PORT = 9160;
	
	@Override
	public void init( Map paramMap) throws ConnectionException {

		logger = LoggerFactory.getLogger(Server.class);

		columnFamily = (String) paramMap.get(COLUMN_FAMILY);
		keySpace     = (String) paramMap.get(KEY_SPACE);
		serverName = "localhost";
		if (paramMap.containsKey(SERVER)) {
			serverName   = (String) paramMap.get(SERVER);
		}
		serverPort = CASSANDRA_PORT;
		if (paramMap.containsKey(PORT)) {
			serverPort   = Integer.parseInt((String) paramMap.get(PORT));
		}
		logger.info("CASSANDRA Connector: " + serverName + ":" + serverPort + " " + keySpace);

		clientPool = new ThriftClientPool(serverName, serverPort, keySpace);

		instanceColumns = false;
		if (paramMap.containsKey(INST_COLUMNS)) {
			instanceColumns = Boolean.valueOf((String) paramMap.get(INST_COLUMNS));
		}

		try {
			//Check db schema		
			TTransport tr = new TSocket(serverName, serverPort);
			TFramedTransport tf = new TFramedTransport(tr);	 
			TProtocol proto = new TBinaryProtocol(tf);	 
			tf.open();
			Client client = new Client(proto);
			KsDef ksDesc = null;
			try {
				ksDesc = client.describe_keyspace(keySpace);				
			} catch (NotFoundException e) {
				String strategy = "org.apache.cassandra.locator.SimpleStrategy";				
				ksDesc = new KsDef(keySpace, strategy, new ArrayList<CfDef>());
				Map stOpts = new HashMap();
				stOpts.put("replication_factor", "1"); //BUG: need to tune this
				ksDesc.setStrategy_options(stOpts);
				try {
					client.system_add_keyspace(ksDesc);
				} catch (Exception e2) {
					throw new ConnectionException("Unable to connect or create keyspace " + keySpace + "\n" + e2.toString());
				}
			} catch (InvalidRequestException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
			tf.close();

		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	@Override
	public void stop() {
	}

	private String getColumnFamily( String inst ) throws ConnectionException {
		if (columns.containsKey(inst)) {
			return columns.get(inst);		
		}
		String cfName;
		if (instanceColumns) {
			cfName = columnFamily + "_" + inst.replaceAll("-", "");
		} else {
			cfName = columnFamily;
		}
		TTransport tr = new TSocket(serverName, serverPort);	 //new default in 0.7 is framed transport	 
		TFramedTransport tf = new TFramedTransport(tr);	 
		try {
			TProtocol proto = new TBinaryProtocol(tf);	 
			tf.open();
			Client client = new Client(proto);
			KsDef ksDesc = client.describe_keyspace(keySpace);				
			Boolean found = false;
			for (CfDef cfdef : ksDesc.getCf_defs()) {
				if (cfdef.name.compareTo(cfName) == 0) {
					found = true;
				}
			}
			if (!found) {
				CfDef cfDesc = new CfDef(keySpace, cfName);
				cfDesc.comparator_type =  "UTF8Type";
				cfDesc.column_type = "Super";
				//ksDesc.addToCf_defs(cfDesc);
				try { 
					client.set_keyspace(keySpace);
					client.system_add_column_family(cfDesc);
				} catch (Exception e2) {
					throw new ConnectionException("Unable to find or create columnFamily " + cfName + "\n" + e2.toString());
				}
			}
			columns.put(inst, cfName);
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidRequestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			tf.close();
		}		
		return columns.get(inst);
	}


	private String stack2column(AppletRef stack) {
		return stack.instance + "/" + stack.pipeline + "/" + stack.applet;
	}


	@Override
	public void addData(AppletRef stack, long jobID, long emitID, String key,
			String data) throws TException {


		final String column = stack2column(stack);
		final ByteBuffer superColumn = ByteBuffer.wrap(key.getBytes());
		final String colName = 
			Long.toString(jobID) + "_" + Long.toString(emitID);
		final ByteBuffer colData = ByteBuffer.wrap(data.getBytes());

		String curCF = columnFamily;
		try {
			curCF = getColumnFamily(stack.instance);
		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}
		final ColumnParent cp = new ColumnParent(curCF);

		ThriftCaller<Boolean> addCall = new ThriftCaller<Boolean>(clientPool)  {
			@Override
			protected Boolean request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {
				cp.setSuper_column(superColumn);
				long clock = System.currentTimeMillis() * 1000;
				//Column col = new Column(ByteBuffer.wrap(colName.getBytes()), 
				//		colData , clock);	
				Column col = new Column(ByteBuffer.wrap(colName.getBytes()));
				col.setValue(colData);
				col.setTimestamp(clock);	
				client.insert(ByteBuffer.wrap(column.getBytes()), cp, col, CL);
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
	public boolean containsKey(AppletRef stack, String key) throws TException {
		final String superColumn = stack2column(stack);
		String curCF = columnFamily;
		try {
			curCF = getColumnFamily(stack.instance);
		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}
		final ColumnPath cp = new ColumnPath(curCF);

		cp.setSuper_column(ByteBuffer.wrap(key.getBytes()));

		ThriftCaller<Boolean> containsCall = new ThriftCaller<Boolean>(clientPool) {
			@Override
			protected Boolean request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {

				boolean returnVal = false;
				try {
					ColumnOrSuperColumn res = client.get(ByteBuffer.wrap(superColumn.getBytes()), cp, CL);
					if (res != null) {
						returnVal = true;
					}
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
	public void deleteStack(AppletRef stack) throws TException {

		final String column = stack2column(stack);
		String curCF = columnFamily;
		try {
			curCF = getColumnFamily(stack.instance);
		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}
		final ColumnPath cp = new ColumnPath(curCF);

		ThriftCaller<Boolean> delCall = new ThriftCaller<Boolean>(clientPool) {
			@Override
			protected Boolean request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {

				long clock = System.currentTimeMillis() * 1000;
				client.remove(ByteBuffer.wrap(column.getBytes()), cp, clock, CL);

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
	public void deleteValue(AppletRef stack, String key) throws TException {
		final String column = stack2column(stack);
		String curCF = columnFamily;
		try {
			curCF = getColumnFamily(stack.instance);
		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}
		final ColumnPath cp = new ColumnPath( curCF );

		cp.setSuper_column(ByteBuffer.wrap(key.getBytes()));

		ThriftCaller<Boolean> delCall = new ThriftCaller<Boolean>(clientPool) {
			@Override
			protected Boolean request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {

				long clock = System.currentTimeMillis() * 1000;
				client.remove(ByteBuffer.wrap(column.getBytes()), cp, clock, CL);
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
	public long getTimeStamp(AppletRef stack) throws TException {
		String superColumn = stack2column(stack);
		String curCF = columnFamily;
		try {
			curCF = getColumnFamily(stack.instance);
		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}

		ThriftSliceIterator<Long> out = new ThriftSliceIterator<Long>(clientPool, superColumn, curCF, "","") {				
			@Override
			void processColumn(ColumnOrSuperColumn scol) {
				for (Column col : scol.getSuper_column().getColumns()) {
					addElement(col.timestamp);
				}
			}
		};

		long timestamp = 0;
		while (out.hasNext()) {
			long cur = out.next();
			if (cur > timestamp) {
				timestamp = cur;
			}
		}
		return timestamp;
	}

	@Override
	public List<String> getValueJSON(AppletRef stack, String key) throws TException {

		final String superColumn = stack2column(stack);
		String curCF = columnFamily;
		try {
			curCF = getColumnFamily(stack.instance);
		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}
		final ColumnPath cp = new ColumnPath(curCF);

		cp.setSuper_column(ByteBuffer.wrap(key.getBytes()));
		ThriftCaller<List<String>> getCall = new ThriftCaller<List<String>>(clientPool) {

			@Override
			protected List<String> request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {
				List<String> out = new LinkedList<String>();
				ColumnOrSuperColumn res;
				try {
					res = client.get(ByteBuffer.wrap(superColumn.getBytes()), cp, CL);
					for (Column col : res.getSuper_column().columns) {
						out.add(new String(col.getValue()));
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

	@Override
	public long keyCount(AppletRef stack, final int maxCount) throws TException {
		final String superColumn = stack2column(stack);
		String curCF = columnFamily;
		try {
			curCF = getColumnFamily(stack.instance);
		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}
		final ColumnParent cp = new ColumnParent(curCF);

		ThriftCaller<Long> getKeyCount = new ThriftCaller<Long>(clientPool) {
			@Override
			protected Long request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {
				SliceRange sRange = new SliceRange(ByteBuffer.wrap("".getBytes()), ByteBuffer.wrap("".getBytes()), false, maxCount);
				SlicePredicate slice = new SlicePredicate();	 
				slice.setSlice_range(sRange);
				long count = client.get_count(ByteBuffer.wrap(superColumn.getBytes()), cp, slice, CL);

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
	public List<String> keySlice(AppletRef stack, final String keyStart, final int count)
	throws TException {
		final String superColumn = stack2column(stack);
		String curCF = columnFamily;
		try {
			curCF = getColumnFamily(stack.instance);
		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}
		final ColumnParent cp = new ColumnParent(curCF);

		ThriftCaller<List<String>> keySliceCall = new ThriftCaller<List<String>>(clientPool) {
			@Override
			protected List<String> request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {
				List<String> out = new ArrayList<String>();
				SliceRange sRange = new SliceRange(ByteBuffer.wrap(keyStart.getBytes()), ByteBuffer.wrap("".getBytes()), false, count);
				SlicePredicate slice = new SlicePredicate();	 
				slice.setSlice_range(sRange);
				List<ColumnOrSuperColumn> res = client.get_slice(ByteBuffer.wrap(superColumn.getBytes()), cp, slice, CL);
				for (ColumnOrSuperColumn col : res) {
					String curKey = new String(col.getSuper_column().getName());
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

	@Override
	public List<KeyValJSONPair> keyValJSONSlice(AppletRef stack, final String startKey,
			final int count) throws TException {

		final String superColumn = stack2column(stack);
		String curCF = columnFamily;
		try {
			curCF = getColumnFamily(stack.instance);
		} catch (ConnectionException e1) {
			e1.printStackTrace();
		}
		final ColumnParent cp = new ColumnParent(curCF);

		ThriftCaller<List<KeyValJSONPair>> keyValSlice = new ThriftCaller<List<KeyValJSONPair>>(clientPool) {			
			@Override
			protected List<KeyValJSONPair> request(Client client)
			throws InvalidRequestException, UnavailableException,
			TimedOutException, TException {

				SliceRange sRange = new SliceRange(ByteBuffer.wrap(startKey.getBytes()), ByteBuffer.wrap("".getBytes()), false, count);
				SlicePredicate slice = new SlicePredicate();	 
				slice.setSlice_range(sRange);
				List<ColumnOrSuperColumn> res = client.get_slice(ByteBuffer.wrap(superColumn.getBytes()), cp, slice, CL);

				List<KeyValJSONPair> out = new LinkedList<KeyValJSONPair>();

				for (ColumnOrSuperColumn scol : res) {		
					String key = new String(scol.getSuper_column().getName());
					for (Column col : scol.getSuper_column().getColumns()) {
						String itemName = new String(col.getName());
						String [] tmp = itemName.split("_");
						long jobID = Long.parseLong(tmp[0]);
						long emitID = Long.parseLong(tmp[1]);
						KeyValJSONPair kv = new KeyValJSONPair(
								key, new String(col.getValue()), jobID, emitID);
						out.add(kv);
					}
				}
				return out;
			}
		};
		try {
			return keyValSlice.call();
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*
		ThriftSliceIterator<KeyValJSONPair> out = new ThriftSliceIterator<KeyValJSONPair>(clientPool, superColumn, curCF, startKey, "", count) {				
			@Override
			void processColumn(ColumnOrSuperColumn scol) {
				String key = new String( scol.getSuper_column().getName() );
				for ( Column col : scol.getSuper_column().getColumns() ) {
					String itemName=new String(col.getName());
					String [] tmp = itemName.split("_");
					long jobID = Long.parseLong(  tmp[0] );
					long emitID = Long.parseLong( tmp[1] );

					KeyValJSONPair kv = new KeyValJSONPair(key, new String(col.getValue()), jobID, emitID);

					addElement( kv );
				}
			}
		};

		LinkedList<KeyValJSONPair> outList = new LinkedList<KeyValJSONPair>();
		for ( KeyValJSONPair kv : out ) {
			outList.add(kv);
		}
		return outList;
		 */

		return null;

	}

	@Override
	public PeerInfo getPeerInfo() {
		PeerInfo out = new PeerInfo();
		out.peerType = PeerType.DB_SERVER;
		return out;
	}


	@Override
	public void start(PluginManager pluginManager) throws Exception {
		// TODO Auto-generated method stub
		
	}

	
}
