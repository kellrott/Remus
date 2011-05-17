package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.mpstore.AttachStore;
import org.mpstore.Serializer;

public class AttachListView implements BaseNode {

	AttachStore attach;	
	String path, instance, key;
	public AttachListView(AttachStore attach, String path, String instance, String key) {
		this.attach = attach;
		this.path = path;
		this.instance = instance;
		this.key = key;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {

		if ( name.length() == 0 ) {
			try {
				for ( String fileName : attach.listAttachment(path, instance, key) ) {
					os.write( serial.dumps(fileName).getBytes() );
					os.write("\n".getBytes());
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			InputStream is = attach.readAttachement(path, instance, key, name);
			if ( is != null ) {
				byte [] buffer = new byte[1024];
				int len;
				try {
					while ( (len = is.read(buffer)) >= 0 ) {
						os.write( buffer, 0, len );
					}
					os.close();
					is.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
			} else {
				throw new FileNotFoundException();
			}
		}

	}

	@Override
	public void doPut(String name, String workerID, Serializer serial, InputStream is, OutputStream os) throws FileNotFoundException {
		if ( name.length() != 0 ) {			
			attach.writeAttachment( path, instance, key, name, is );
		}
	}
	
	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
