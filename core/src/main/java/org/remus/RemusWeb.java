package org.remus;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;
import org.remus.core.BaseStackNode;
import org.remus.mapred.MapReduceCallback;
import org.remus.thrift.AppletRef;
import org.remus.thrift.JobStatus;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.WorkMode;

public abstract class RemusWeb extends RemusPeer {

	abstract public RemusAttach getAttachStore();
	abstract public RemusDB getDataStore();
	abstract public void jsRequest(String string, WorkMode map, 
			BaseStackNode appletView,
			MapReduceCallback mapReduceCallback);

	

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
	public String jobRequest(String dataServer, String attachServer, WorkDesc work)
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
	public void scheduleRequest() throws NotImplemented, TException {
		throw new NotImplemented();
	}

	@Override
	public void writeBlock(AppletRef stack, String key, String name,
			long offset, ByteBuffer data) throws NotImplemented, TException {
		throw new NotImplemented();
	}
	
	@Override
	public String scheduleInfoJSON() throws NotImplemented, TException {
		throw new NotImplemented();
	}
	
	@Override
	public int jobCancel(String jobID) throws NotImplemented, TException {
		throw new NotImplemented();	
	}
}
