package org.mpstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.remus.RemusApp;

public class FileAttachStore implements AttachStore {

	
	File basePath = null;
	@Override
	public void initAttachStore(Map paramMap) {
		this.basePath = new File( (String)paramMap.get(RemusApp.configWork) );
	}

	
	@Override
	public InputStream readAttachement(String path, String instance, String key, String attachment) {
		try {
			File attachFile = NameFlatten.flatten(basePath, path, instance, key, attachment);
			if ( attachFile.exists() ) {
				InputStream is = new FileInputStream( attachFile );
				return is;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void writeAttachment(String path, String instance, String key, String attachment,
			InputStream inputStream) {
		try {
			File attachFile = NameFlatten.flatten(basePath, path, instance, key, attachment);
			if ( !attachFile.getParentFile().exists() ) {
				attachFile.getParentFile().mkdirs();
			}
			FileOutputStream fos = new FileOutputStream(attachFile);
			byte [] buffer = new byte[1024];
			int len;
			while ((len=inputStream.read(buffer))>0) {
				fos.write(buffer, 0, len);
			}
			fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public List<String> listAttachment(String path, String instance, String key) {
		File attachDir = NameFlatten.flatten(basePath, path, instance, key, null).getParentFile();
		LinkedList<String> out = new LinkedList<String>();
		for ( File file : attachDir.listFiles() ) {
			out.add( file.getName() );
		}
		return out;
	}

	
}
