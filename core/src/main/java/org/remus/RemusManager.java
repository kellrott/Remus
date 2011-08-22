package org.remus;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;
import org.remus.plugin.PluginInterface;
import org.remus.thrift.AppletRef;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.JobStatus;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.RemusNet;
import org.remus.thrift.WorkDesc;

abstract public class RemusManager implements RemusNet.Iface, PluginInterface {


	@Override
	public void addData(AppletRef stack, long jobID, long emitID, String key,
			String data) throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public boolean containsKey(AppletRef stack, String key)
			throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public void deleteAttachment(AppletRef stack, String key, String name)
			throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public void deleteStack(AppletRef stack) throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public void deleteValue(AppletRef stack, String key) throws NotImplemented,
			TException {
		throw new NotImplemented();		
	}

	@Override
	public long getAttachmentSize(AppletRef stack, String key, String name)
			throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public long getTimeStamp(AppletRef stack) throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public List<String> getValueJSON(AppletRef stack, String key)
			throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public boolean hasAttachment(AppletRef stack, String key, String name)
			throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public void initAttachment(AppletRef stack, String key, String name,
			long length) throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public String jobRequest(String dataServer, WorkDesc work)
			throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public JobStatus jobStatus(String jobID) throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public long keyCount(AppletRef stack, int maxCount) throws NotImplemented,
			TException {
		throw new NotImplemented();		
	}

	@Override
	public List<String> keySlice(AppletRef stack, String keyStart, int count)
			throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public List<KeyValJSONPair> keyValJSONSlice(AppletRef stack,
			String startKey, int count) throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public List<String> listAttachments(AppletRef stack, String key)
			throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public ByteBuffer readBlock(AppletRef stack, String key, String name,
			long offset, int length) throws NotImplemented, TException {
		throw new NotImplemented();		
	}

	@Override
	public void writeBlock(AppletRef stack, String key, String name,
			long offset, ByteBuffer data) throws NotImplemented, TException {
		throw new NotImplemented();	
	}

	@Override
	public void addPeer(PeerInfoThrift info) throws NotImplemented,
			BadPeerName, TException {
		throw new NotImplemented();	
	}

	@Override
	public void delPeer(String peerName) throws NotImplemented, TException {
		throw new NotImplemented();	
	}

	@Override
	public List<PeerInfoThrift> getPeers() throws NotImplemented, TException {
		throw new NotImplemented();	
	}
	
	@Override
	public int jobCancel(String jobID) throws NotImplemented, TException {
		throw new NotImplemented();	
	}
}
