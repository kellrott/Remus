package org.remusNet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.thrift.TException;
import org.remusNet.thrift.AppletRef;
import org.remusNet.thrift.RemusAttachThrift;

public abstract class RemusAttach implements RemusAttachThrift.Iface {

	static public final int BLOCK_SIZE=4048; 
	@SuppressWarnings("unchecked")
	abstract public void init(Map params);

	public void copyTo(AppletRef stack, String key, String name, File file) throws TException, IOException {
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

	public void copyFrom(AppletRef stack, String key, String name, File file) throws TException, IOException {
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
		long offset;
		long readLen;
		AppletRef stack;
		String key;
		String name;
		public BlockReader(AppletRef stack, String key, String name) throws TException {
			this.stack = stack;
			this.name = name;
			this.key = key;
			fileSize = getAttachmentSize(stack, key, name);
			offset = 0;
			readLen = 0;
			buffer = null;
		}
		@Override
		public int read() throws IOException {
			if (readLen >= fileSize) {
				return -1;
			}
			if (buffer == null || offset >= buffer.length) {
				try {
					ByteBuffer buf = readBlock(stack, key, name, offset, BLOCK_SIZE);
					buffer = buf.array();
					offset = 0;
				} catch (TException e) {
					throw new IOException(e);
				}
			}
			byte out = buffer[(int) offset];
			offset++;
			return out;
		}

	}

	public InputStream readAttachement(AppletRef stack, String key,
			String name) {
		try {
			return new BlockReader(stack, key, name);
		} catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}


	public void writeAttachment(AppletRef ar, String key, String name, InputStream is) {
		// TODO Auto-generated method stub

	}

}
