package org.remus;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.mpstore.Serializer;
import org.remus.serverNodes.BaseNode;
import org.remus.work.RemusApplet;

public class PipelineListView implements BaseNode {

	RemusPipeline pipe;
	PipelineListView( RemusPipeline applet ) {
		this.pipe = applet;
	}

	@Override
	public void doDelete(Map params) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID, Serializer serial,
			OutputStream os) throws FileNotFoundException {
		Map out = new HashMap();
		if ( name.length() == 0 ) {
			for ( KeyValuePair kv : pipe.getDataStore().listKeyPairs("/" + pipe.id + "/@pipeline", RemusInstance.STATIC_INSTANCE_STR)) {
				out.put(kv.getKey(), kv.getValue() );
			}
		} else {
			for ( Object obj : pipe.getDataStore().get("/" + pipe.id + "/@pipeline", RemusInstance.STATIC_INSTANCE_STR, name )) {
				out.put(name, obj );
			}
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
		if ( pipe != null ) {
			try {
				StringBuilder sb = new StringBuilder();
				byte [] buffer = new byte[1024];
				int len;
				while( (len=is.read(buffer)) > 0 ) {
					sb.append(new String(buffer, 0, len));
				}
				System.err.println( sb.toString() );
				Object data = serial.loads(sb.toString());
				pipe.getApp().putApplet(pipe, name, data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
