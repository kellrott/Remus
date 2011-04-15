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
import org.remus.work.RemusApplet;

public class PipelineListView implements BaseNode {

	RemusPipeline applet;
	PipelineListView( RemusPipeline applet ) {
		this.applet = applet;
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
			for ( KeyValuePair kv : applet.getDataStore().listKeyPairs("/" + applet.id + "@pipeline", RemusInstance.STATIC_INSTANCE_STR)) {
				out.put(kv.getKey(), kv.getValue() );
			}
		} else {
			for ( Object obj : applet.getDataStore().get("/" + applet.id + "@pipeline", RemusInstance.STATIC_INSTANCE_STR, name )) {
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
	public void doPut(InputStream is, OutputStream os) {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
