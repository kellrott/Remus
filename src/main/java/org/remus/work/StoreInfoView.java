package org.remus.work;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.MPStore;
import org.mpstore.Serializer;
import org.remus.BaseNode;
import org.remus.server.RemusApp;

@SuppressWarnings("unchecked")
public class StoreInfoView implements BaseNode {

	RemusApp app;
	public StoreInfoView(RemusApp app) {
		this.app = app;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {

		try {
			Map out = new HashMap();
			out.put( "dataStore", app.getRootDatastore().getConfig()  );
			out.put( "attachStore", app.getRootAttachStore().getConfig()  );
			
			os.write( serial.dumps( out ).getBytes() );
		} catch ( IOException e ) {

		}
	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public BaseNode getChild(String name) {
		return null;
	}

}
