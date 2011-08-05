package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusDB;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.server.RemusDatabaseException;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class PipelineInstanceStatusView implements BaseNode {

	RemusPipeline pipeline;
	RemusInstance inst;
	RemusDB datastore;
	public PipelineInstanceStatusView(RemusPipeline pipeline, RemusInstance inst, RemusDB datastore) {
		this.pipeline = pipeline;
		this.inst = inst;
		this.datastore = datastore;
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
				for ( String appletName : pipeline.getMembers() ) {
					AppletRef ap = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, appletName + "/@instance" );
					try {
						for (Object data : datastore.get(ap, inst.toString())) {
							Map out = new HashMap();
							out.put(appletName, data);
							os.write(JSON.dumps(out).getBytes());
							os.write("\n".getBytes());
						}
					} catch (TException e) {
						e.printStackTrace();
					} catch (NotImplemented e) {
						e.printStackTrace();
					}
				}
			} else {
				try {
					RemusApplet applet = pipeline.getApplet(name);
					if (applet != null) {
						AppletRef ap = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@instance" );
						for (Object data : applet.getDataStore().get(ap, inst.toString())) {
							Map out = new HashMap();
							out.put(applet.getID(), data);
							os.write(JSON.dumps(out).getBytes());
							os.write("\n".getBytes());
						}
					}
				} catch (TException e) {
					e.printStackTrace();
				} catch (NotImplemented e) {
					e.printStackTrace();
				} catch (RemusDatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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
