package org.remus;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.plugin.PluginInterface;
import org.remus.thrift.AppletRef;
import org.remus.thrift.AttachmentInfo;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.RemusNet.Iface;

abstract public class RemusWorker extends RemusPeer {


	@Override
	public void addDataJSON(AppletRef stack, long jobID, long emitID, String key,
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
	public void initAttachment(AppletRef stack, String key, String name) 
	throws NotImplemented, TException {
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
	public void appendBlock(AppletRef stack, String key, String name, ByteBuffer data) 
	throws NotImplemented, TException {
		throw new NotImplemented();
	}

	@Override
	public List<String> stackSlice(String startKey, int count)
	throws NotImplemented, TException {
		throw new NotImplemented();
	}

	@Override
	public AttachmentInfo getAttachmentInfo(AppletRef stack, String key,
			String name) throws NotImplemented, TException {
		throw new NotImplemented();	
	}
}
