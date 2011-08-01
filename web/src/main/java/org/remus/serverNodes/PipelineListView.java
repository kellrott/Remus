package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.server.RemusDatabaseException;
import org.remus.server.RemusPipelineImpl;
import org.remusNet.JSON;
import org.remusNet.KeyValPair;
import org.remusNet.thrift.AppletRef;

public class PipelineListView implements BaseNode {

	RemusPipelineImpl pipe;
	public PipelineListView( RemusPipelineImpl applet ) {
		this.pipe = applet;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os) throws FileNotFoundException {
		Map out = new HashMap();
		AppletRef ar = new AppletRef(pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
		try {
			if ( name.length() == 0 ) {
				for ( KeyValPair kv : pipe.getDataStore().listKeyPairs(ar)) {
					out.put(kv.getKey(), kv.getValue() );
				}
			} else {
				for ( Object obj : pipe.getDataStore().get(ar, name )) {
					out.put(name, obj );
				}
			}
		} catch (TException e) {
			e.printStackTrace();
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
		if ( pipe != null ) {
			try {
				StringBuilder sb = new StringBuilder();
				byte [] buffer = new byte[1024];
				int len;
				while( (len=is.read(buffer)) > 0 ) {
					sb.append(new String(buffer, 0, len));
				}
				System.err.println( sb.toString() );
				Object data = JSON.loads(sb.toString());
				pipe.getApp().putApplet(pipe, name, data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
