package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusDB;
import org.remus.core.BaseNode;
import org.remus.core.RemusApp;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class PipelineConfigView implements BaseNode {	
	RemusApp app;
	RemusDB datastore;
	PipelineConfigView(RemusApp app, RemusDB datastore) {
		this.app = app;
		this.datastore = datastore;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		RemusPipeline pipeline = app.getPipeline(name);
		if ( pipeline != null ) {
			try {
				app.deletePipeline( pipeline );
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os)
	throws FileNotFoundException {
		Map out = new HashMap();		
		AppletRef ar = new AppletRef(null, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
		for ( KeyValPair kv : datastore.listKeyPairs( ar ) ) {
			out.put(kv.getKey(), kv.getValue());
		}		
		try {
			os.write( JSON.dumps(out).getBytes() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	

	}

	@Override
	public void doPut(String name, String workerID, InputStream is, OutputStream os) throws FileNotFoundException {
		try {
			StringBuilder sb = new StringBuilder();
			byte [] buffer = new byte[1024];
			int len;
			while( (len=is.read(buffer)) > 0 ) {
				sb.append(new String(buffer, 0, len));
			}
			System.err.println( sb.toString() );
			Object data = JSON.loads(sb.toString());
			app.putPipeline(name, data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		return null;
	}

}
