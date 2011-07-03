package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.mpstore.Serializer;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.work.RemusApplet;

public class PipelineErrorView implements BaseNode {

	RemusPipeline pipeline;
	public PipelineErrorView(RemusPipeline remusPipeline) {
		this.pipeline = remusPipeline;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		for ( RemusApplet applet : pipeline.getMembers() ) {
			for ( RemusInstance inst : applet.getInstanceList() ) {
				applet.deleteErrors(inst);
			}
		}		
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		for ( RemusApplet applet : pipeline.getMembers() ) {
			for ( RemusInstance inst : applet.getInstanceList() ) {
				Map<String,Map<String,Object>> out = new HashMap<String, Map<String,Object>>();				
				for ( KeyValuePair kv : applet.getDataStore().listKeyPairs( applet.getPath() + "/@error", inst.toString() ) ) {
					String key = inst.toString() + ":" + applet.getID();
					if ( ! out.containsKey( key )) {
						out.put(key, new HashMap<String,Object>() );
					}
					out.get( key ).put(kv.getKey(), kv.getValue() );
				}
				if ( out.size() > 0 ) {
					try {
						os.write( serial.dumps( out ).getBytes() );
						os.write( "\n".getBytes() );
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}				
				}
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
