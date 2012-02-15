package org.remus;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.remus.thrift.AttachmentInfo;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;
import org.remus.thrift.RemusNet.Iface;
import org.remus.thrift.TableRef;

public class RemusRemote implements RemusNet.Iface {
	public static final int REMOTE_TIMEOUT = 60000;
	
	private Byte [] lock = new Byte[0];
	private Iface iface;
	private String host = null;
	private int port = 0;

	public RemusRemote(String host, int port) throws TTransportException {
		this.iface = null;
		this.host = host;
		this.port = port;
		checkIface();
	}

	private void checkIface() throws TTransportException {
		if (iface == null) {
			TSocket transport = new TSocket(host, port);
			TBinaryProtocol protocol = new TBinaryProtocol(transport);
			transport.open();
			transport.setTimeout(REMOTE_TIMEOUT);
			iface = new RemusNet.Client(protocol);
		}
	}

	@Override
	public void addDataJSON(TableRef stack, 
			String key, String data) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			iface.addDataJSON(stack, key, data);
		}
	}


	@Override
	public boolean containsKey(TableRef stack, String key)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.containsKey(stack, key);
		}
	}

	@Override
	public void deleteAttachment(TableRef stack, String key, String name)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			iface.deleteAttachment(stack, key, name);
		}
	}

	@Override
	public void deleteTable(TableRef stack) throws NotImplemented,
	TException {
		synchronized (lock) {
			checkIface();
			iface.deleteTable(stack);
		}
	}
	

	@Override
	public List<String> getValueJSON(TableRef stack, String key)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.getValueJSON(stack, key);
		}
	}

	@Override
	public boolean hasAttachment(TableRef stack, String key, String name)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.hasAttachment(stack, key, name);
		}	
	}

	@Override
	public void initAttachment(TableRef stack, String key, String name) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			iface.initAttachment(stack, key, name);		
		}
	}


	@Override
	public long keyCount(TableRef stack, int maxCount)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.keyCount(stack, maxCount);
		}
	}

	@Override
	public List<String> keySlice(TableRef stack, String keyStart, int count)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.keySlice(stack, keyStart, count);
		}
	}

	@Override
	public List<KeyValJSONPair> keyValJSONSlice(TableRef stack,
			String startKey, int count) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.keyValJSONSlice(stack, startKey, count);
		}
	}

	@Override
	public List<String> listAttachments(TableRef stack, String key)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.listAttachments(stack, key);
		}
	}

	@Override
	public ByteBuffer readBlock(TableRef stack, String key, String name,
			long offset, int length) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.readBlock(stack, key, name, offset, length);
		}			
	}

	@Override
	public void appendBlock(TableRef stack, String key, String name, ByteBuffer data) 
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			iface.appendBlock(stack, key, name, data);
		}
	}

	
	@Override
	public List<String> tableSlice(String startKey, int count)
			throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.tableSlice(startKey, count);
		}
	}

	public void close() {
		//((RemusNet.Client) iface)..getInputProtocol().getTransport().close();
		iface = null;
	}

	@Override
	public AttachmentInfo getAttachmentInfo(TableRef stack, String key,
			String name) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.getAttachmentInfo(stack, key, name);
		}
	}

	@Override
	public void createTable(TableRef table) throws NotImplemented, TException {
		// TODO Auto-generated method stub
		
	}

}
