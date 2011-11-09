package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.RemusWeb;
import org.remus.core.BaseStackNode;
import org.remus.core.DataStackNode;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.WorkMode;

public class PipelineInstanceListViewer implements BaseNode {

	private RemusWeb web;
	private RemusPipeline pipeline;

	public PipelineInstanceListViewer(RemusPipeline pipeline, RemusWeb web) {
		this.web = web;
		this.pipeline = pipeline;
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

		AppletRef ar = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, Constants.INSTANCE_APPLET);
		for ( KeyValPair kv : web.getDataStore().listKeyPairs(ar) ) {
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
			final OutputStream os) throws FileNotFoundException {


		char [] buffer = new char[1024];
		int len;
		StringBuilder sb = new StringBuilder();
		Reader in = new InputStreamReader(is);
		try {
			do {
				len = in.read(buffer);
				if (len > 0) {
					sb.append(buffer, 0, len);
				}
			} while (len >= 0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		AppletRef ar = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, Constants.INSTANCE_APPLET);
		DataStackNode ds = new DataStackNode(web.getDataStore(), ar);

		web.jsRequest( sb.toString(), WorkMode.MAP, ds, new 
				BaseStackNode() {

			@Override
			public List<String> keySlice(String keyStart, int count) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public List<String> getValueJSON(String key) {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void delete(String key) {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean containsKey(String key) {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public void add(String key, String data) {
				Map out = new HashMap();
				out.put(key, JSON.loads(data));
				try {
					os.write(JSON.dumps(out).getBytes());
					os.write("\n".getBytes());						
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}



	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
