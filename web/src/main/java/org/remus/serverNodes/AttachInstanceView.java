package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.remus.JSON;
import org.remus.RemusAttach;
import org.remus.RemusDatabaseException;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;

public class AttachInstanceView implements BaseNode {

	RemusPipeline pipe;
	RemusInstance inst;
	RemusAttach attachstore;
	public AttachInstanceView(RemusPipeline pipeline, RemusInstance inst, RemusAttach attachstore) {
		this.pipe = pipeline;
		this.inst = inst;
		this.attachstore = attachstore;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {		

		if ( name.length() == 0)  {
			for ( String appletName : pipe.getMembers() ) {
				try {
					os.write( JSON.dumps( appletName ).getBytes() );
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
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		try {
			RemusApplet applet = pipe.getApplet(name);
			if ( applet != null ) {
				return new AttachAppletView( pipe, applet, inst, attachstore );
			}
		} catch (RemusDatabaseException e) {
			e.printStackTrace();
		}
		return null;
	}

}
