package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.server.RemusPipelineImpl;
import org.remus.work.RemusAppletImpl;
import org.remusNet.JSON;
import org.remusNet.KeyValPair;
import org.remusNet.thrift.AppletRef;

public class PipelineErrorView implements BaseNode {

	RemusPipelineImpl pipeline;
	public PipelineErrorView(RemusPipelineImpl remusPipeline) {
		this.pipeline = remusPipeline;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		for ( RemusAppletImpl applet : pipeline.getMembers() ) {
			for ( RemusInstance inst : applet.getInstanceList() ) {
				try {
				applet.deleteErrors(inst);
				} catch (TException e) {
					e.printStackTrace();
					throw new FileNotFoundException();
				}
			}
		}		
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {
		for ( RemusAppletImpl applet : pipeline.getMembers() ) {
			for ( RemusInstance inst : applet.getInstanceList() ) {
				Map<String,Map<String,Object>> out = new HashMap<String, Map<String,Object>>();		
				
				AppletRef ar = new AppletRef(applet.getPipeline().getID(), inst.toString(), applet.getID() + "/@error" );
				
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
