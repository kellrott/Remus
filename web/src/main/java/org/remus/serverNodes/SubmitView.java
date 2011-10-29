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
import org.remus.RemusWeb;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;

public class SubmitView implements BaseNode {

	RemusPipeline pipe;
	RemusDB datasource;
	RemusWeb parent;
	public SubmitView(RemusPipeline pipe, RemusDB datasource, RemusWeb parent) {
		this.pipe = pipe;
		this.datasource = datasource;
		this.parent = parent;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		try {
			if (name.length() > 0) {
				AppletRef ar = new AppletRef( pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, Constants.SUBMIT_APPLET );		
				datasource.deleteValue(ar, name);
			}	
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {

		AppletRef ar = new AppletRef( pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, Constants.SUBMIT_APPLET );
		try {
			if (name.length() == 0) {
				for (KeyValPair kv : datasource.listKeyPairs(ar)) {
					Map out = new HashMap();					
					out.put(kv.getKey(), kv.getValue());
					try {
						os.write(JSON.dumps(out).getBytes());
						os.write("\n".getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} else {
				Map out = new HashMap();
				for (Object obj : datasource.get(ar, name)) {
					out.put(name, obj);
				}
				try {
					os.write(JSON.dumps(out).getBytes());
					os.write("\n".getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			e.printStackTrace();
		}
	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		if (name.length() != 0) {
			try {
				StringBuilder sb = new StringBuilder();
				byte [] buffer = new byte[1024];
				int len;
				while ((len = is.read(buffer)) > 0) {
					sb.append(new String(buffer, 0, len));
				}
				Object data = JSON.loads(sb.toString());
				RemusNet.Iface master = parent.getMaster();
				AppletRef subAR = new AppletRef(pipe.getID(), Constants.STATIC_INSTANCE, Constants.SUBMIT_APPLET);
				master.addDataJSON(subAR, 0, 0, name, JSON.dumps(data));
				for (String oJson : master.getValueJSON(subAR, name)) {
					Object o = JSON.loads(oJson);
					os.write(JSON.dumps(((Map)o).get("_instance") + " created").getBytes());
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
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
