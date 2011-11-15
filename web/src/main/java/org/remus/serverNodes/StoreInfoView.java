package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusWeb;
import org.remus.core.RemusApp;
import org.remus.core.TableUtils;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;

@SuppressWarnings("unchecked")
public class StoreInfoView implements BaseNode {

	RemusWeb web;
	public StoreInfoView(RemusWeb web) {
		this.web = web;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {

		RemusNet.Iface db = web.getDataStore();
		
		try {
			for (String arName : db.stackSlice("", 500)) {
				Map out = new HashMap();
				AppletRef ar = TableUtils.StringToRef(arName);
				out.put("_instance", ar.instance);
				out.put("_pipeline", ar.pipeline);
				out.put("_applet", ar.applet);
				os.write(JSON.dumps(out).getBytes());
				os.write("\n".getBytes());
				
			}
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
		throw new FileNotFoundException();
	}

	@Override
	public BaseNode getChild(String name) {
		return null;
	}

}
