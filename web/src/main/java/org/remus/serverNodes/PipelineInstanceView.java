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
import org.remus.core.BaseNode;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class PipelineInstanceView implements BaseNode {

	RemusPipeline pipeline;
	RemusInstance inst;
	RemusDB datastore;

	public PipelineInstanceView( RemusPipeline pipeline, RemusInstance inst, RemusDB datastore ) {
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
		if ( name.length() == 0)  {
			for ( String appletName : pipeline.getMembers() ) {
				AppletRef ar = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, appletName + AppletInstanceStatusView.InstanceStatusName );
				try {
					for ( Object instObj : datastore.get(ar, inst.toString() ) ) {
						Map out = new HashMap();
						out.put( appletName, instObj );	
						try {
							os.write( JSON.dumps( out ).getBytes() );
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
		RemusApplet applet = pipeline.getApplet(name);
		if ( applet != null ) {
			return new AppletInstanceView(pipeline, applet, inst);
		}

		if ( name.compareTo("@error") == 0 ) {
			return new InstanceErrorView(pipeline, inst);
		}

		if ( name.compareTo("@status") == 0 ) {
			return new PipelineInstanceStatusView(pipeline, inst, datastore);
		}

		if ( name.compareTo("@attach") == 0 ) {
			return new AttachInstanceView(pipeline, inst);			
		}

		if ( name.compareTo("@query") == 0 ) {
			return new PipelineInstanceQueryView(pipeline,inst);
		}

		return null;		
	}

}
