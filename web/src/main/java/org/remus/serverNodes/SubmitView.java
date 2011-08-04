package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusDB;
import org.remus.core.BaseNode;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.work.Submission;

public class SubmitView implements BaseNode {

	RemusPipeline pipe;
	RemusDB datasource;
	public SubmitView(RemusPipeline pipe, RemusDB datasource) {
		this.pipe = pipe;
		this.datasource = datasource;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {

		Map out = new HashMap();
		AppletRef ar = new AppletRef( pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit" );
		try {
			if ( name.length() == 0 ) {
				for ( KeyValPair kv : datasource.listKeyPairs(ar)) {
					out.put(kv.getKey(), kv.getValue() );
				}
			} else {
				for ( Object obj : datasource.get(ar, name )) {
					out.put(name, obj );
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
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
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		if ( name.length() != 0 ) {
			try {
				StringBuilder sb = new StringBuilder();
				byte [] buffer = new byte[1024];
				int len;
				while( (len=is.read(buffer)) > 0 ) {
					sb.append(new String(buffer, 0, len));
				}
				Object data = JSON.loads(sb.toString());
				RemusInstance inst = pipe.handleSubmission(name, (Map)data);
				os.write( JSON.dumps( inst.toString() + " created" ).getBytes() );
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
