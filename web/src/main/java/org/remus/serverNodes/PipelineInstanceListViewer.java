package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;

public class PipelineInstanceListViewer implements BaseNode {

	RemusPipeline pipeline;
	RemusDB datastore;
	public PipelineInstanceListViewer(RemusPipeline pipeline, RemusDB datastore ) {
		this.pipeline = pipeline;
		this.datastore = datastore;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		System.err.println("DELETE:" + name); 
		try {
			pipeline.deleteInstance(new RemusInstance(name));
		} catch (RemusDatabaseException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {

		AppletRef ar = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance");
		for ( KeyValPair kv : datastore.listKeyPairs(ar) ) {
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
