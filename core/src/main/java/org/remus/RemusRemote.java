package org.remus;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.remus.thrift.AppletRef;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.JobStatus;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.RemusNet;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.RemusNet.Iface;

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
	public void addData(AppletRef stack, long jobID, long emitID,
			String key, String data) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			iface.addData(stack, jobID, emitID, key, data);
		}
	}


	@Override
	public boolean containsKey(AppletRef stack, String key)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.containsKey(stack, key);
		}
	}

	@Override
	public void deleteAttachment(AppletRef stack, String key, String name)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			iface.deleteAttachment(stack, key, name);
		}
	}

	@Override
	public void deleteStack(AppletRef stack) throws NotImplemented,
	TException {
		synchronized (lock) {
			checkIface();
			iface.deleteStack(stack);
		}
	}

	@Override
	public void deleteValue(AppletRef stack, String key)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			iface.deleteValue(stack, key);
		}
	}

	@Override
	public long getAttachmentSize(AppletRef stack, String key, String name)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.getAttachmentSize(stack, key, name);
		}	
	}


	@Override
	public long getTimeStamp(AppletRef stack) throws NotImplemented,
	TException {
		synchronized (lock) {
			checkIface();
			return iface.getTimeStamp(stack);
		}
	}

	@Override
	public List<String> getValueJSON(AppletRef stack, String key)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.getValueJSON(stack, key);
		}
	}

	@Override
	public boolean hasAttachment(AppletRef stack, String key, String name)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.hasAttachment(stack, key, name);
		}	
	}

	@Override
	public void initAttachment(AppletRef stack, String key, String name,
			long length) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			iface.initAttachment(stack, key, name, length);		
		}
	}

	@Override
	public int jobCancel(String jobID) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.jobCancel(jobID);
		}
	}

	@Override
	public String jobRequest(String dataServer, String attachServer,
			WorkDesc work) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.jobRequest(dataServer, attachServer, work);
		}
	}

	@Override
	public JobStatus jobStatus(String jobID) throws NotImplemented,
	TException {
		synchronized (lock) {
			checkIface();
			return iface.jobStatus(jobID);
		}
	}

	@Override
	public long keyCount(AppletRef stack, int maxCount)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.keyCount(stack, maxCount);
		}
	}

	@Override
	public List<String> keySlice(AppletRef stack, String keyStart, int count)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.keySlice(stack, keyStart, count);
		}
	}

	@Override
	public List<KeyValJSONPair> keyValJSONSlice(AppletRef stack,
			String startKey, int count) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.keyValJSONSlice(stack, startKey, count);
		}
	}

	@Override
	public List<String> listAttachments(AppletRef stack, String key)
	throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.listAttachments(stack, key);
		}
	}

	@Override
	public ByteBuffer readBlock(AppletRef stack, String key, String name,
			long offset, int length) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.readBlock(stack, key, name, offset, length);
		}			
	}

	@Override
	public String scheduleInfoJSON() throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			return iface.scheduleInfoJSON();
		}
	}

	@Override
	public void scheduleRequest() throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			iface.scheduleRequest();
		}
	}

	@Override
	public String status() throws TException {
		synchronized (lock) {
			checkIface();
			return iface.status();
		}
	}

	@Override
	public void writeBlock(AppletRef stack, String key, String name,
			long offset, ByteBuffer data) throws NotImplemented, TException {
		synchronized (lock) {
			checkIface();
			iface.writeBlock(stack, key, name, offset, data);
		}
	}

	@Override
	public List<PeerInfoThrift> peerInfo(List<PeerInfoThrift> info)
	throws NotImplemented, BadPeerName, TException {
		synchronized (lock) {
			checkIface();
			return iface.peerInfo(info);
		}
	}

	public void close() {
		((RemusNet.Client) iface).getInputProtocol().getTransport().close();
		iface = null;
	}

}
