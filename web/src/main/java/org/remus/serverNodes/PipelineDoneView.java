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
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;

public class PipelineDoneView implements BaseNode {


	private RemusPipeline pipe;
	private RemusDB datastore;

	public PipelineDoneView(RemusPipeline pipe, RemusDB datastore) {
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
		if (name.length() > 0) {
			String [] tmp = name.split(":");
			if (tmp.length == 2) {
				AppletRef doneTable = new AppletRef(pipe.getID(), tmp[0], tmp[1] + Constants.DONE_APPLET);
				for (KeyValPair kv : datastore.listKeyPairs(doneTable)) {
					Map out = new HashMap();
					out.put(kv.getKey(), kv.getValue());
					try {
						os.write(JSON.dumps(out).getBytes());
						os.write("\n".getBytes());
					} catch (IOException e) {
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
