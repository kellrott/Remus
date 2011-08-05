package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.server.RemusDatabaseException;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class PipelineErrorView implements BaseNode {

	RemusPipeline pipeline;
	public PipelineErrorView(RemusPipeline remusPipeline) {
		this.pipeline = remusPipeline;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		for ( String appletName : pipeline.getMembers() ) {
			try {
				RemusApplet applet = pipeline.getApplet(appletName);
				for ( RemusInstance inst : applet.getInstanceList() ) {
					try {
						applet.deleteErrors(inst);
					} catch (TException e) {
						e.printStackTrace();
						throw new FileNotFoundException();
					} catch (NotImplemented e) {
						e.printStackTrace();
						throw new FileNotFoundException();
					}
				}
			} catch (RemusDatabaseException e) {
				e.printStackTrace();
			}
		}		
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {
		for ( String appletName : pipeline.getMembers() ) {
			try {
				RemusApplet applet = pipeline.getApplet(appletName);
				for ( RemusInstance inst : applet.getInstanceList() ) {
					Map<String,Map<String,Object>> out = new HashMap<String, Map<String,Object>>();		

					AppletRef ar = new AppletRef(pipeline.getID(), inst.toString(), applet.getID() + "/@error" );

					for ( KeyValPair kv : applet.getDataStore().listKeyPairs(ar) ) {
						String key = inst.toString() + ":" + applet.getID();
						if ( ! out.containsKey( key )) {
							out.put(key, new HashMap<String,Object>() );
						}
						out.get( key ).put(kv.getKey(), kv.getValue() );
					}
					if ( out.size() > 0 ) {
						try {
							os.write( JSON.dumps( out ).getBytes() );
							os.write( "\n".getBytes() );
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}				
					}
				}
			} catch (RemusDatabaseException e) {
				e.printStackTrace();
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
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
