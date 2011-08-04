package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.core.BaseNode;
import org.remus.core.RemusInstance;
import org.remus.server.RemusPipelineImpl;
import org.remus.thrift.AppletRef;
import org.remus.work.RemusAppletImpl;

public class PipelineInstanceStatusView implements BaseNode {

	RemusPipelineImpl pipeline;
	RemusInstance inst;
	public PipelineInstanceStatusView(RemusPipelineImpl pipeline, RemusInstance inst) {
		this.pipeline = pipeline;
		this.inst = inst;
	}


	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {
		try {

			if ( name.length() == 0 ) {
				for ( RemusAppletImpl applet : pipeline.getMembers() ) {
					AppletRef ap = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@instance" );
					try {
						for ( Object data : applet.getDataStore().get( ap, inst.toString() ) ) {
							Map out = new HashMap();
							out.put(applet.getID(), data);
							os.write( JSON.dumps(out).getBytes() );
							os.write("\n".getBytes() );
						}
					} catch (TException e) {
						e.printStackTrace();
					}
				}
			} else {
				RemusAppletImpl applet = pipeline.getApplet( name );
				if ( applet != null ) {
					try {
						AppletRef ap = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@instance" );
						for ( Object data : applet.getDataStore().get( ap, inst.toString() ) ) {
							Map out = new HashMap();
							out.put(applet.getID(), data);
							os.write( JSON.dumps(out).getBytes() );
							os.write("\n".getBytes() );
						}
					} catch (TException e) {
						e.printStackTrace();
					}
				}
			}

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
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
