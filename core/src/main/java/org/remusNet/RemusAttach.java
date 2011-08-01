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
		while ( (size = fis.read(buffer)) > 0 ) {
			ByteBuffer buff = ByteBuffer.allocate(size);
			for ( int i =0 ;i < size; i++) {
				buff.put( buffer[i] );
			}
			writeBlock(stack, key, name, offset, buff );
			offset += size;
		}
		fis.close();
	}

	public void copyFrom(AppletRef stack, String key, String name, File file) throws TException, IOException {
		long fileSize = getAttachmentSize(stack, key, name);
		
		FileOutputStream fos = new FileOutputStream(file);
		
		long offset = 0;
		while ( offset < fileSize ) {
			ByteBuffer buf = readBlock(stack, key, name, offset, BLOCK_SIZE);
			fos.write( buf.array() );
			offset += buf.array().length;
		}
	}

	public InputStream readAttachement(AppletRef ar, String string,
			String string2) {
		// TODO Auto-generated method stub
		return null;
	}


	public void writeAttachment(AppletRef ar, String key, String name, InputStream is) {
		// TODO Auto-generated method stub
		
	}
	
}
