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
import org.remus.core.BaseNode;
import org.remus.core.RemusInstance;
import org.remus.thrift.AppletRef;
import org.remus.work.RemusAppletImpl;

public class InstanceListView implements BaseNode {
	RemusAppletImpl applet;
	public InstanceListView(RemusAppletImpl applet) {
		this.applet = applet;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {

		Map out = new HashMap();
		AppletRef ar = new AppletRef(applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@instance");
		try {
			if ( name.length() == 0 ) {
				for ( KeyValPair kv : applet.getDataStore().listKeyPairs(ar)) {
					out.put(kv.getKey(), kv.getValue() );
				}
			} else {
				for ( Object obj : applet.getDataStore().get( ar, name )) {
					out.put(name, obj );
				}
			}
		} catch (TException e ) {
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
