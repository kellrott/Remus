package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.server.RemusPipelineImpl;
import org.remusNet.JSON;
import org.remusNet.KeyValPair;
import org.remusNet.thrift.AppletRef;

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
			OutputStream os) throws FileNotFoundException {
		
		AppletRef ar = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance");
		for ( KeyValPair kv : pipeline.getDataStore().listKeyPairs(ar) ) {
			Map out = new HashMap();
			out.put( kv.getKey(), kv.getValue() );	
			try {
				os.write( JSON.dumps( out ).getBytes() );
				os.write("\n".getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
