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
import org.remus.core.DataStackInfo;
import org.remus.core.RemusInstance;
import org.remus.thrift.AppletRef;
import org.remus.work.RemusAppletImpl;

public class PipelineAppletAgentView implements BaseNode {

	RemusAppletImpl applet;
	public PipelineAppletAgentView(RemusAppletImpl applet) {
		this.applet = applet;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {

		if ( params.containsKey(DataStackInfo.PARAM_FLAG) ) {
			try {
				os.write( JSON.dumps( DataStackInfo.formatInfo(PipelineAppletAgentView.class, "status", applet.getPipeline()) ).getBytes() );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}

		AppletRef ar = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@instance" );
		try {
			if ( name.length() == 0 ) {
				for ( String key : applet.getDataStore().listKeys( ar ) ) {
					try {
						os.write( JSON.dumps( key + ":" + applet.getID() ).getBytes() );
						os.write( "\n".getBytes() );
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}			
				}
			} else {
				String tmp[] = name.split(":");
				if ( tmp.length == 2 && tmp[1].compareTo( applet.getID() ) == 0 ) {
					for ( Object obj : applet.getDataStore().get( ar, tmp[0] ) ) {
						Map out = new HashMap();
						out.put( name, obj );				
						try {
							os.write( JSON.dumps(out).getBytes() );
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
		} catch (TException e) {
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
		// TODO Auto-generated method stub
		return null;
	}

}
