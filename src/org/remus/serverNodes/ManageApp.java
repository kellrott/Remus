package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.mpstore.Serializer;

public class ManageApp implements BaseNode {

	@Override
	public void doDelete(Map params) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		
		if ( name.compareTo("") == 0 )
			name = "manage.html";
		InputStream is = ManageApp.class.getResourceAsStream( name );
		if ( is == null ) {
			throw new FileNotFoundException();
		} else {
			try {
				byte [] buffer = new byte[1024];
				int len;
				while ( (len = is.read(buffer)) >= 0 ) {
					os.write( buffer, 0, len );
				}
				os.flush();
				os.close();
				is.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}				
	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}