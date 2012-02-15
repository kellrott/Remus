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
import org.remus.thrift.AttachmentInfo;
import org.remus.thrift.KeyValJSONPair;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;
import org.remus.thrift.TableRef;

public abstract class RemusAttach implements RemusNet.Iface {

	static public final int BLOCK_SIZE=1048576; 
	
	
	public static RemusAttach wrap(final RemusNet.Iface attach) {
		if (attach == null) {
			return null;
		}
		if (attach instanceof RemusAttach) {
			return (RemusAttach) attach;
		}
		return new RemusAttach() {
					
			
			@Override
			public ByteBuffer readBlock(TableRef stack, String key, String name,
					long offset, int length) throws NotImplemented, TException {
				return attach.readBlock(stack, key, name, offset, length);
			}
			
			@Override
			public List<String> listAttachments(TableRef stack, String key)
					throws NotImplemented, TException {
				return attach.listAttachments(stack, key);
			}
			
			@Override
			public void initAttachment(TableRef stack, String key, String name) throws NotImplemented, TException {
				attach.initAttachment(stack, key, name);				
			}
			
			@Override
			public boolean hasAttachment(TableRef stack, String key, String name)
					throws NotImplemented, TException {
				return attach.hasAttachment(stack, key, name);
			}
			
			@Override
			public void deleteTable(TableRef stack) throws NotImplemented, TException {
				attach.deleteTable(stack);
			}
			
			@Override
			public void deleteAttachment(TableRef stack, String key, String name)
					throws NotImplemented, TException {
				attach.deleteAttachment(stack, key, name);
			}
			
			@Override
			public void init(Map params) {}

			
			@Override
			public void appendBlock(TableRef stack, String key, String name,
					ByteBuffer data) throws NotImplemented, TException {
				attach.appendBlock(stack, key, name, data);
			}

			@Override
			public AttachmentInfo getAttachmentInfo(TableRef stack, String key,
					String name) throws NotImplemented, TException {
				return attach.getAttachmentInfo(stack, key, name);
			}
			
		};
	}
	
	
	@SuppressWarnings("unchecked")
	abstract public void init(Map params);

	public long copyTo(File file, TableRef stack, String key, String name) throws TException, IOException, NotImplemented {
		initAttachment(stack, key, name);		
		byte [] buffer = new byte[BLOCK_SIZE];
		int size;
		long total = 0;
		FileInputStream fis = new FileInputStream(file);
		while ((size = fis.read(buffer)) > 0) {
			ByteBuffer buff = ByteBuffer.wrap(buffer, 0, size);
			appendBlock(stack, key, name, buff);
			total += size;
		}
		fis.close();
		return total;
	}

	public long copyFrom(File file, TableRef stack, String key, String name) throws TException, IOException, NotImplemented {
		AttachmentInfo info = getAttachmentInfo(stack, key, name);
		long fileSize = info.size;
		
		FileOutputStream fos = new FileOutputStream(file);

		long offset = 0;
		while (offset < fileSize) {
			ByteBuffer buf = readBlock(stack, key, name, offset, BLOCK_SIZE);
			fos.write(buf.array(), 0, buf.limit());
			offset += buf.limit();
		}
		return fileSize;
	}

	private class BlockReader extends InputStream {
		long fileSize;
		byte [] buffer;
		long offset, fileOffset;
		TableRef stack;
		String key;
		String name;
		public BlockReader(TableRef stack, String key, String name) throws TException, NotImplemented {
			this.stack = stack;
			this.name = name;
			this.key = key;
			AttachmentInfo info = getAttachmentInfo(stack, key, name);
			fileSize = info.size;
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

	public InputStream readAttachment(TableRef stack, String key,
			String name) throws NotImplemented {
		try {
			return new BlockReader(stack, key, name);
		} catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}

	
	private class SendOnClose extends OutputStream {
		private TableRef stack;
		private String key;
		private String name;
		private File file;
		private FileOutputStream fos;
		
		public SendOnClose(TableRef stack, String key, String name) throws IOException {
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
				initAttachment(stack, key, name);
				byte [] buffer = new byte[BLOCK_SIZE];
				long offset = 0;
				FileInputStream fis = new FileInputStream(file);
				while (offset < fileSize) {
					int readSize = fis.read(buffer);
					ByteBuffer buff = ByteBuffer.allocate(readSize);
					for (int i = 0; i < readSize; i++) {
						buff.array()[i] = buffer[i];
					}
					appendBlock(stack, key, name, buff);
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
	

	public OutputStream writeAttachment(TableRef stack, String key, String name) throws IOException {
		return new SendOnClose(stack, key, name);		
	}
	
	@Override
	public void addDataJSON(TableRef stack, String key,
			String data) throws NotImplemented, TException {
		throw new NotImplemented();
	}

	@Override
	public boolean containsKey(TableRef stack, String key)
			throws NotImplemented, TException {
		throw new NotImplemented();
	}
	
	@Override
	public List<String> getValueJSON(TableRef stack, String key)
			throws NotImplemented, TException {
		throw new NotImplemented();
	}
	

	@Override
	public long keyCount(TableRef stack, int maxCount) throws NotImplemented,
			TException {
		throw new NotImplemented();
	}

	@Override
	public List<String> keySlice(TableRef stack, String keyStart, int count)
			throws NotImplemented, TException {
		throw new NotImplemented();
	}

	@Override
	public List<KeyValJSONPair> keyValJSONSlice(TableRef stack,
			String startKey, int count) throws NotImplemented, TException {
		throw new NotImplemented();
	}
	

	@Override
	public void createTable(TableRef table) throws NotImplemented,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<String> tableSlice(String startKey, int count)
			throws NotImplemented, TException {
		// TODO Auto-generated method stub
		return null;
	}
	
}
