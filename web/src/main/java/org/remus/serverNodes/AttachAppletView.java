package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.work.RemusAppletImpl;
import org.remusNet.JSON;
import org.remusNet.thrift.AppletRef;

public class AttachAppletView implements BaseNode {

	RemusAppletImpl applet;
	RemusInstance inst;

	public AttachAppletView(RemusAppletImpl applet, RemusInstance inst) {
		this.applet = applet;
		this.inst = inst;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {
		if ( name.length() == 0 ) {
			AppletRef ap = new AppletRef( applet.getPipeline().getID(), inst.toString(), applet.getID() );
			for ( String key : applet.getDataStore().listKeys( ap ) ) {
				try {
					os.write( JSON.dumps( key ).getBytes() );
					os.write("\n".getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			throw new FileNotFoundException();
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
		return new AttachListView(applet.getAttachStore(), applet.getPipeline(), inst, applet.getID(), name );
	}

}
