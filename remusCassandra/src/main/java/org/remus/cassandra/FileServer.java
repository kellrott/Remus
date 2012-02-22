package org.remus.cassandra;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import org.remus.RemusInterface;
import org.remus.thrift.AttachmentInfo;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.TableRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class FileServer extends RemusInterface {

	/**
	 * Location in the file system the attachments are stored
	 */
	private File basePath;
	/**
	 * Is the directory shared?
	 */
	private Boolean dirShared;
	private Logger logger;
	public final static String DIR_NAME    = "dir";
	public final static String DIR_SHARED  = "shared";

	@Override
	public void init(Map params) {
		logger = LoggerFactory.getLogger(FileServer.class);
		this.basePath = new File((String) params.get(DIR_NAME));
		if (params.containsKey(DIR_SHARED)) {
			this.dirShared = Boolean.valueOf(params.get(DIR_SHARED).toString());
		} else {
			this.dirShared = false;
		}
	}

	@Override
	public void deleteAttachment(TableRef stack, String key, String name)
			throws TException {
		File attachFile = NameFlatten.flatten(basePath, stack.instance, stack.table, key, name);
		attachFile.delete();
	}

	public static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			for (File child : dir.listFiles()) {
				boolean success = deleteDir(child);
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete();
	}

	@Override
	public void deleteTable(TableRef stack) throws TException {
		logger.debug("DELETE ATTACH STACK:" + stack);
		File attachFile = NameFlatten.flatten(basePath, stack.instance, stack.table, null, null);
		File stackDir = attachFile.getParentFile().getParentFile();
		deleteDir(stackDir);
	}

	@Override
	public boolean hasAttachment(TableRef stack, String key, String name)
			throws TException {
		File attachFile = NameFlatten.flatten(basePath, stack.instance, stack.table, key, name);
		if (attachFile.exists()) {
			return true;
		}
		return false;
	}

	@Override
	public void initAttachment(TableRef stack, String key, String name) throws TException {
		File attachFile = NameFlatten.flatten(basePath, stack.instance, stack.table, key, name);
		try {
			if (!attachFile.getParentFile().exists()) {
				attachFile.getParentFile().mkdirs();
			}
			RandomAccessFile f = new RandomAccessFile(attachFile, "rw");
			f.setLength(0);
			f.close();
			logger.debug("Init file:" + stack + " " + key + " " + name);			
		} catch (FileNotFoundException e) {
			throw new TException(e);
		} catch (IOException e) {
			throw new TException(e);
		}
	}

	@Override
	public List<String> listAttachments(TableRef stack, String key)
			throws TException {
		File attachDir = NameFlatten.flatten(basePath, stack.instance, stack.table, key, null).getParentFile();
		LinkedList<String> out = new LinkedList<String>();
		if ( attachDir.exists() ) {
			for ( File file : attachDir.listFiles() ) {
				out.add( file.getName() );
			}
		}
		return out;
	}

	@Override
	public ByteBuffer readBlock(TableRef stack, String key, String name,
			long offset, int length) throws TException {
		File attachFile = NameFlatten.flatten(basePath, stack.instance, stack.table, key, name);
		try {
			RandomAccessFile f = new RandomAccessFile(attachFile, "rw");
			f.seek(offset);
			byte data [] = new byte[length];
			int readLen = f.read( data, 0, length );		
			f.close();
			if (readLen > 0) {
				return ByteBuffer.wrap(data, 0, readLen);
			}
			return ByteBuffer.wrap(data, 0, 0);
		} catch (FileNotFoundException e) {
			throw new TException(e);
		} catch (IOException e) {
			throw new TException(e);
		}
	}

	@Override
	public void appendBlock(TableRef stack, String key, String name, ByteBuffer data) throws TException {
		File attachFile = NameFlatten.flatten(basePath, stack.instance, stack.table, key, name);
		try {
			FileOutputStream f = new FileOutputStream(attachFile, true);
			byte [] array = data.array();
			f.write(array, data.arrayOffset(), data.limit() - data.arrayOffset());
			f.close();
			//logger.debug("Appending block " + stack + " " + key + " " + name + " length: " + array.length);
		} catch (FileNotFoundException e) {
			throw new TException(e);
		} catch (IOException e) {
			throw new TException(e);
		}		
	}

	@Override
	public AttachmentInfo getAttachmentInfo(TableRef stack, String key,
			String name) throws NotImplemented, TException {
		File attachFile = NameFlatten.flatten(basePath, stack.instance, stack.table, key, name);
		AttachmentInfo out = new AttachmentInfo(name);
		if (attachFile.exists()) {
			out.setSize(attachFile.length());
			out.setExists(true);
		} else {
			out.setExists(false);
		}
		return out;
	}

}
