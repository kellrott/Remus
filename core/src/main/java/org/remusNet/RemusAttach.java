package org.remusNet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
	
}
