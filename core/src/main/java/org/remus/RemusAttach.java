package org.remus;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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
import org.remus.thrift.RemusNet;
import org.remus.thrift.WorkDesc;

public abstract class RemusAttach implements RemusNet.Iface, PluginInterface {

	static public final int BLOCK_SIZE=4048; 
	@SuppressWarnings("unchecked")
	abstract public void init(Map params);

	public void copyTo(AppletRef stack, String key, String name, File file) throws TException, IOException, NotImplemented {
		long fileSize = file.length();		
		initAttachment(stack, key, name, fileSize);		
		byte [] buffer = new byte[BLOCK_SIZE];
		int size;
		long offset = 0;
		FileInputStream fis = new FileInputStream(file);
		while ((size = fis.read(buffer)) > 0) {
			ByteBuffer buff = ByteBuffer.allocate(size);
			for (int i = 0; i < size; i++) {
				buff.put(buffer[i]);
			}
			writeBlock(stack, key, name, offset, buff);
			offset += size;
		}
		fis.close();
	}

	public void copyFrom(AppletRef stack, String key, String name, File file) throws TException, IOException, NotImplemented {
		long fileSize = getAttachmentSize(stack, key, name);

		FileOutputStream fos = new FileOutputStream(file);

		long offset = 0;
		while (offset < fileSize) {
			ByteBuffer buf = readBlock(stack, key, name, offset, BLOCK_SIZE);
			fos.write(buf.array());
			offset += buf.array().length;
		}
	}

	private class BlockReader extends InputStream {
		long fileSize;
		byte [] buffer;
		long offset, fileOffset;
		AppletRef stack;
		String key;
		String name;
		public BlockReader(AppletRef stack, String key, String name) throws TException, NotImplemented {
			this.stack = stack;
			this.name = name;
			this.key = key;
			fileSize = getAttachmentSize(stack, key, name);
			offset = 0;
			fileOffset = 0;
			buffer = null;
		}
		@Override
		public int read() throws IOException {
			if (fileOffset >= fileSize) {
				return -1;
			}
			if (buffer == null || offset >= buffer.length) {
				try {
					ByteBuffer buf = readBlock(stack, key, name, fileOffset, BLOCK_SIZE);
					buffer = buf.array();
					offset = 0;
				} catch (TException e) {
					throw new IOException(e);
				} catch (NotImplemented e) {
					throw new IOException(e);
				}
			}
			byte out = buffer[(int) offset];
			offset++;
			fileOffset++;
			return out;
		}

	}

	public InputStream readAttachement(AppletRef stack, String key,
			String name) throws NotImplemented {
		try {
			return new BlockReader(stack, key, name);
		} catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}

	
	private class SendOnClose extends OutputStream {
		private AppletRef stack;
		private String key;
		private String name;
		private File file;
		private FileOutputStream fos;
		
		public SendOnClose(AppletRef stack, String key, String name) throws IOException {
			this.stack = stack;
			this.key = key;
			this.name = name;
			file = File.createTempFile("remus", "trans");
			fos = new FileOutputStream(file);
		}
		
		@Override
		public void write(int b) throws IOException {
			fos.write(b);			
		}
		
		@Override
		public void close() throws IOException {
			super.close();
			fos.close();
			long fileSize = file.length();
			try {
				initAttachment(stack, key, name, fileSize);
				byte [] buffer = new byte[BLOCK_SIZE];
				long offset = 0;
				FileInputStream fis = new FileInputStream(file);
				while (offset < fileSize) {
					int readSize = fis.read(buffer);
					ByteBuffer buff = ByteBuffer.allocate(readSize);
					for (int i = 0; i < readSize; i++) {
						buff.array()[i] = buffer[i];
					}
					writeBlock(stack, key, name, offset, buff);
					offset += readSize;
				}
				fis.close();
				file.delete();
			} catch (TException e) {
				throw new IOException(e);
			} catch (NotImplemented e) {
				throw new IOException(e);
			}
		}		
	}
	

	public OutputStream writeAttachment(AppletRef stack, String key, String name) throws IOException {
		return new SendOnClose(stack, key, name);		
	}

	

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
	public String jobRequest(String dataServer, WorkDesc work)
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
	public void scheduleRequest() throws NotImplemented, TException {
		throw new NotImplemented();	
	}

	@Override
	public JobStatus jobStatus(String jobID) throws NotImplemented,
			TException {
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

	@Override
	public Map<String, String> scheduleInfo() throws NotImplemented, TException {
		throw new NotImplemented();
	}

	@Override
	public int jobCancel(String jobID) throws NotImplemented, TException {
		throw new NotImplemented();	
	}
}
