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
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;

public class PipelineWorkView implements BaseNode {

	private RemusPipeline pipe;
	private RemusDB datastore;

	public PipelineWorkView(RemusPipeline pipe, RemusDB datastore) {
		this.pipe = pipe;
		this.datastore = datastore;
	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os)
			throws FileNotFoundException {

		if (name.length() == 0) {
			AppletRef workRef = new AppletRef(pipe.getID(), Constants.STATIC_INSTANCE, Constants.WORK_APPLET);
			for (KeyValPair kv : datastore.listKeyPairs(workRef)) {
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
			if (name.contains("/")) { 
				String [] tmp1 = name.split("/");
				String [] tmp2 = tmp1[0].split(":");
				AppletRef workRef = new AppletRef(pipe.getID(), tmp2[0], tmp2[1] + Constants.WORK_APPLET);
				try {
					for (Object obj : datastore.get(workRef, tmp1[1])) {
						try {
							os.write(JSON.dumps(obj).getBytes());
							os.write("\n".getBytes());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} catch (NotImplemented e) {

				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				String [] tmp = name.split(":");
				AppletRef workRef = new AppletRef(pipe.getID(), tmp[0], tmp[1] + Constants.WORK_APPLET);
				for (KeyValPair kv : datastore.listKeyPairs(workRef)) {
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
	public void doDelete(String name, Map params, String workerID)
			throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

}
