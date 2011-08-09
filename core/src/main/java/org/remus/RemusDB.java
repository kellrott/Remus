package org.remus;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.plugin.PluginInterface;
import org.remus.thrift.AppletRef;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.JobStatus;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.RemusNet.Iface;

public abstract class RemusDB implements Iface, PluginInterface {

	abstract public void init(Map params) throws ConnectionException;

	public void add( AppletRef stack, long jobID, long emitID, String key, Object object ) throws TException, NotImplemented {
		addData(stack, jobID,emitID, key, JSON.dumps(object));
	}
	
	
	public List<Object> get(AppletRef stack, String key)
			throws TException, NotImplemented {
		
		List<String> i = getValueJSON(stack, key);

		List<Object> out = new ArrayList<Object>(i.size());
		for ( String j : i ) {
			out.add(JSON.loads(j));
		}
		return out;
	}
	
	public List<KeyValPair> keyValSlice(AppletRef stack,
			String startKey, int count) throws TException, NotImplemented {
		List<KeyValJSONPair> i = keyValJSONSlice(stack, startKey, count);
		
		List<KeyValPair> out = new ArrayList<KeyValPair>( i.size() );
		for ( KeyValJSONPair kv : i ) {
			out.add( new KeyValPair(kv) );
		}
		return out;
	}

	public Iterable<String> listKeys(AppletRef applet) {
		return new RemusDBSliceIterator<String>(this, applet, "", "", false) {
			@Override
			void processKeyValue(String key, Object val, long jobID, long emitID) {
				addElement(key);
			}			
		};		
	}

	public Iterable<KeyValPair> listKeyPairs(AppletRef applet) {	
		return new RemusDBSliceIterator<KeyValPair>(this, applet, "", "", true) {
			@Override
			void processKeyValue(String key, Object val, long jobID, long emitID) {
				addElement(new KeyValPair(key, val, jobID, emitID));
			}			
		};		
	}
	


	@Override
	public void deleteAttachment(AppletRef stack, String key, String name)
			throws NotImplemented, TException {
		throw new NotImplemented();
	}


	@Override
	public long getAttachmentSize(AppletRef stack, String key, String name)
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
			throws TException, NotImplemented {
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
	public void scheduleRequest() throws TException, NotImplemented {
		throw new NotImplemented();
	}


	@Override
	public JobStatus jobStatus(String jobID) throws TException, NotImplemented {
		throw new NotImplemented();
	}


	@Override
	public void writeBlock(AppletRef stack, String key, String name,
			long offset, ByteBuffer data) throws NotImplemented, TException {
		throw new NotImplemented();
	}

	
	@Override
	public void addPeer(PeerInfoThrift info) throws BadPeerName, TException, NotImplemented {
		throw new NotImplemented();
	}


	@Override
	public void delPeer(String peerName) throws TException, NotImplemented {
		throw new NotImplemented();
	}


	@Override
	public List<PeerInfoThrift> getPeers() throws TException, NotImplemented {
		throw new NotImplemented();
	}

	
}
