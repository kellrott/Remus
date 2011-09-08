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

	public static RemusNet.Iface getClient(String host, int port) throws TTransportException {
		TSocket transport = new TSocket(host, port);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		transport.open();
		RemusNet.Client client = new RemusNet.Client(protocol);
		return new RemusRemote(client);
	}

	private Iface iface;

	public RemusRemote(RemusNet.Iface i) {
		this.iface = i;
	}

	@Override
	public void addData(AppletRef stack, long jobID, long emitID,
			String key, String data) throws NotImplemented, TException {
		synchronized (iface) {
			iface.addData(stack, jobID, emitID, key, data);
		}
	}

	@Override
	public void addPeer(PeerInfoThrift info) throws NotImplemented,
	BadPeerName, TException {
		synchronized (iface) {
			iface.addPeer(info);
		}
	}

	@Override
	public boolean containsKey(AppletRef stack, String key)
	throws NotImplemented, TException {
		synchronized (iface) {
			return iface.containsKey(stack, key);
		}
	}

	@Override
	public void delPeer(String peerName) throws NotImplemented, TException {
		synchronized (iface) {
			iface.delPeer(peerName);
		}
	}

	@Override
	public void deleteAttachment(AppletRef stack, String key, String name)
	throws NotImplemented, TException {
		synchronized (iface) {
			iface.deleteAttachment(stack, key, name);
		}
	}

	@Override
	public void deleteStack(AppletRef stack) throws NotImplemented,
	TException {
		synchronized (iface) {
			iface.deleteStack(stack);
		}
	}

	@Override
	public void deleteValue(AppletRef stack, String key)
	throws NotImplemented, TException {
		synchronized (iface) {
			iface.deleteValue(stack, key);
		}
	}

	@Override
	public long getAttachmentSize(AppletRef stack, String key, String name)
	throws NotImplemented, TException {
		synchronized (iface) {
			return iface.getAttachmentSize(stack, key, name);
		}	
	}

	@Override
	public List<PeerInfoThrift> getPeers() throws NotImplemented,
	TException {
		synchronized (iface) {
			return iface.getPeers();
		}
	}

	@Override
	public long getTimeStamp(AppletRef stack) throws NotImplemented,
	TException {
		synchronized (iface) {
			return iface.getTimeStamp(stack);
		}
	}

	@Override
	public List<String> getValueJSON(AppletRef stack, String key)
	throws NotImplemented, TException {
		synchronized (iface) {
			return iface.getValueJSON(stack, key);
		}
	}

	@Override
	public boolean hasAttachment(AppletRef stack, String key, String name)
	throws NotImplemented, TException {
		synchronized (iface) {
			return iface.hasAttachment(stack, key, name);
		}	
	}

	@Override
	public void initAttachment(AppletRef stack, String key, String name,
			long length) throws NotImplemented, TException {
		synchronized (iface) {
			iface.initAttachment(stack, key, name, length);		
		}
	}

	@Override
	public int jobCancel(String jobID) throws NotImplemented, TException {
		synchronized (iface) {
			return iface.jobCancel(jobID);
		}
	}

	@Override
	public String jobRequest(String dataServer, String attachServer,
			WorkDesc work) throws NotImplemented, TException {
		synchronized (iface) {
			return iface.jobRequest(dataServer, attachServer, work);
		}
	}

	@Override
	public JobStatus jobStatus(String jobID) throws NotImplemented,
	TException {
		synchronized (iface) {
			return iface.jobStatus(jobID);
		}
	}

	@Override
	public long keyCount(AppletRef stack, int maxCount)
	throws NotImplemented, TException {
		synchronized (iface) {
			return iface.keyCount(stack, maxCount);
		}
	}

	@Override
	public List<String> keySlice(AppletRef stack, String keyStart, int count)
	throws NotImplemented, TException {
		synchronized (iface) {
			return iface.keySlice(stack, keyStart, count);
		}
	}

	@Override
	public List<KeyValJSONPair> keyValJSONSlice(AppletRef stack,
			String startKey, int count) throws NotImplemented, TException {
		synchronized (iface) {
			return iface.keyValJSONSlice(stack, startKey, count);
		}
	}

	@Override
	public List<String> listAttachments(AppletRef stack, String key)
	throws NotImplemented, TException {
		synchronized (iface) {
			return iface.listAttachments(stack, key);
		}
	}

	@Override
	public ByteBuffer readBlock(AppletRef stack, String key, String name,
			long offset, int length) throws NotImplemented, TException {
		synchronized (iface) {
			return iface.readBlock(stack, key, name, offset, length);
		}			
	}

	@Override
	public String scheduleInfoJSON() throws NotImplemented, TException {
		synchronized (iface) {
			return iface.scheduleInfoJSON();
		}
	}

	@Override
	public void scheduleRequest() throws NotImplemented, TException {
		synchronized (iface) {
			iface.scheduleRequest();
		}
	}

	@Override
	public String status() throws TException {
		synchronized (iface) {
			return iface.status();
		}
	}

	@Override
	public void writeBlock(AppletRef stack, String key, String name,
			long offset, ByteBuffer data) throws NotImplemented, TException {
		synchronized (iface) {
			iface.writeBlock(stack, key, name, offset, data);
		}
	}

}
