package org.remus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.mpstore.Serializer;
import org.remus.serverNodes.BaseNode;

public class PipelineView implements BaseNode {	
	RemusApp app;
	PipelineView(RemusApp app) {
		this.app = app;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) {
		RemusPipeline pipeline = app.getPipeline(name);
		if ( pipeline != null ) {
			app.deletePipeline( pipeline );
		}
	}

	@Override
	public void doGet(String name, Map params, String workerID, Serializer serial, OutputStream os)
	throws FileNotFoundException {
		Map out = new HashMap();		
		for ( KeyValuePair kv : app.getRootDatastore().listKeyPairs( "/@pipeline", RemusInstance.STATIC_INSTANCE_STR ) ) {
			out.put(kv.getKey(), kv.getValue());
		}		
		try {
			os.write( serial.dumps(out).getBytes() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	

	}

	@Override
	public void doPut(String name, String workerID, Serializer serial, InputStream is, OutputStream os) {
		try {
			StringBuilder sb = new StringBuilder();
			byte [] buffer = new byte[1024];
			int len;
			while( (len=is.read(buffer)) > 0 ) {
				sb.append(new String(buffer, 0, len));
			}
			System.err.println( sb.toString() );
			Object data = serial.loads(sb.toString());
			app.putPipeline( name, data );					
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		return null;
	}

}
