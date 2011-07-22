package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.mpstore.Serializer;
import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.server.RemusPipelineImpl;

public class PipelineInstanceListViewer implements BaseNode {

	RemusPipelineImpl pipeline;
	public PipelineInstanceListViewer(RemusPipelineImpl pipeline) {
		this.pipeline = pipeline;
	}
	
	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		System.err.println("DELETE:" + name);
		pipeline.deleteInstance(new RemusInstance(name));		
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		
		for ( KeyValuePair kv : pipeline.getDataStore().listKeyPairs( "/" + pipeline.getID() + "/@instance", RemusInstance.STATIC_INSTANCE_STR) ) {
			Map out = new HashMap();
			out.put( kv.getKey(), kv.getValue() );	
			try {
				os.write( serial.dumps( out ).getBytes() );
				os.write("\n".getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

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
