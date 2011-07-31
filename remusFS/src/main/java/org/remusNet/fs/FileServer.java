package org.remusNet.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import org.remusNet.RemusAttach;
import org.remusNet.thrift.AppletRef;

public class FileServer extends RemusAttach {

	private File basePath;
	private Boolean dirShared;
	public final static String DIR_NAME    = "org.mpstore.FileAttachStore.dir";
	public final static String DIR_SHARED  = "org.mpstore.FileAttachStore.shared";

	@Override
	public void init(Map params) {
		this.basePath = new File( (String)params.get( DIR_NAME ) );
		this.dirShared = Boolean.valueOf( (String) params.get( DIR_SHARED ) ); 		
	}

	@Override
	public void deleteAttachment(AppletRef stack, String key, String name)
			throws TException {
		File attachFile = NameFlatten.flatten(basePath, stack.pipeline, stack.instance, stack.applet, key, name);
		attachFile.delete();
	}

	@Override
	public boolean hasAttachment(AppletRef stack, String key, String name)
			throws TException {
		File attachFile = NameFlatten.flatten(basePath, stack.pipeline, stack.instance, stack.applet, key, name);
		if ( attachFile.exists() ) {
			return true;
		}
		return false;
	}

	@Override
	public void initAttachment(AppletRef stack, String key, String name,
			long length) throws TException {
		File attachFile = NameFlatten.flatten(basePath, stack.pipeline, stack.instance, stack.applet, key, name);
        try {
			RandomAccessFile f = new RandomAccessFile(attachFile, "rw");
			f.setLength(length);
			f.close();
        } catch (FileNotFoundException e) {
			throw new TException(e);
		} catch (IOException e) {
			throw new TException(e);
		}
	}

	@Override
	public List<String> listAttachments(AppletRef stack, String key)
			throws TException {
		File attachDir = NameFlatten.flatten(basePath, stack.pipeline, stack.instance, stack.applet, key, null).getParentFile();
		LinkedList<String> out = new LinkedList<String>();
		if ( attachDir.exists() ) {
			for ( File file : attachDir.listFiles() ) {
				out.add( file.getName() );
			}
		}
		return out;
	}

	@Override
	public ByteBuffer readBlock(AppletRef stack, String key, String name,
			long offset, int length) throws TException {
		File attachFile = NameFlatten.flatten(basePath, stack.pipeline, stack.instance, stack.applet, key, name);
        try {
			RandomAccessFile f = new RandomAccessFile(attachFile, "rw");
			f.seek(offset);
			ByteBuffer buffer = ByteBuffer.allocate(length);
			f.read( buffer.array(), 0, length );			
			f.close();
			return buffer;
        } catch (FileNotFoundException e) {
			throw new TException(e);
		} catch (IOException e) {
			throw new TException(e);
		}
	}

	@Override
	public void writeBlock(AppletRef stack, String key, String name,
			long offset, ByteBuffer data) throws TException {
		File attachFile = NameFlatten.flatten(basePath, stack.pipeline, stack.instance, stack.applet, key, name);
        try {
			RandomAccessFile f = new RandomAccessFile(attachFile, "rw");
			f.seek(offset);
			f.write( data.array() );
			f.close();
        } catch (FileNotFoundException e) {
			throw new TException(e);
		} catch (IOException e) {
			throw new TException(e);
		}		
	}

}
