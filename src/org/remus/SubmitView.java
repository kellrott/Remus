package org.remus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.mpstore.Serializer;
import org.remus.serverNodes.BaseNode;
import org.remus.work.RemusApplet;
import org.remus.work.Submission;

public class SubmitView implements BaseNode {

	RemusPipeline pipe;
	public SubmitView(RemusPipeline pipe) {
		this.pipe = pipe;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {

		Map out = new HashMap();
		if ( name.length() == 0 ) {
			for ( KeyValuePair kv : pipe.getDataStore().listKeyPairs("/" + pipe.id + "/@submit", RemusInstance.STATIC_INSTANCE_STR)) {
				out.put(kv.getKey(), kv.getValue() );
			}
		} else {
			for ( Object obj : pipe.getDataStore().get("/" + pipe.id + "/@submit", RemusInstance.STATIC_INSTANCE_STR, name )) {
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
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {
		if ( name.length() != 0 ) {
			try {
				StringBuilder sb = new StringBuilder();
				byte [] buffer = new byte[1024];
				int len;
				while( (len=is.read(buffer)) > 0 ) {
					sb.append(new String(buffer, 0, len));
				}
				Object data = serial.loads(sb.toString());
				RemusInstance inst = new RemusInstance();
				if ( ((Map)data).containsKey( Submission.AppletField ) ) {
					List<String> aList = (List)((Map)data).get(Submission.AppletField);
					inst = pipe.setupInstance( name, (Map)data, aList );					
				} else {
					inst = pipe.setupInstance( name, (Map)data, new LinkedList() );					

				}
				
				((Map)data).put(Submission.InstanceField, inst.toString());
				pipe.getDataStore().add( "/" + pipe.getID() + "/@submit", 
						RemusInstance.STATIC_INSTANCE_STR, 
						(Long)0L, 
						(Long)0L, 
						name,
						data );		
				pipe.getDataStore().add( "/" + pipe.getID() + "/@instance", 
						RemusInstance.STATIC_INSTANCE_STR, 
						0L, 0L,
						inst.toString(),
						name);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
