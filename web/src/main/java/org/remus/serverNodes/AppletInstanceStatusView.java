package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.work.RemusAppletImpl;
import org.remusNet.JSON;
import org.remusNet.KeyValPair;
import org.remusNet.thrift.AppletRef;

public class AppletInstanceStatusView implements BaseNode {

	public static final String InstanceStatusName = "/@instance";

	RemusAppletImpl applet;
	public AppletInstanceStatusView(RemusAppletImpl applet) {
		this.applet = applet;
	}


	public Object getStatus(RemusInstance inst) {
		Object statObj = null;
		AppletRef ar = new AppletRef(applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + InstanceStatusName );
		try {
			for ( Object obj : applet.getDataStore().get( ar, inst.toString()) ) {
				statObj = obj;
			}
		} catch ( TException e ) {
			e.printStackTrace();
		}
		return statObj;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {

		AppletRef ar = new AppletRef(applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + InstanceStatusName );
		if ( name.length() == 0 ) {
			for ( KeyValPair kv : applet.getDataStore().listKeyPairs( ar ) ) {			
				Map out = new HashMap();
				out.put( kv.getKey(), kv.getValue() );	
				try {
					os.write( JSON.dumps( out ).getBytes() );
					os.write("\n".getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}		
		} else {
			try {
				for ( Object obj : applet.getDataStore().get( ar, name) ) {
					Map out = new HashMap();
					out.put(name, obj );				
					try {
						os.write( JSON.dumps(out).getBytes() );
						os.write("\n".getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}catch (TException e) {
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


	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void updateStatus(RemusAppletImpl applet,
			RemusInstance inst, Map update) {
		Object statObj = null;
		AppletRef ar = new AppletRef(applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + InstanceStatusName );

		try {
			for ( Object obj : applet.getDataStore().get( ar, inst.toString()) ) {
				statObj = obj;
			}
			if ( statObj == null ) {
				statObj = new HashMap();
			}
			for ( Object key : update.keySet() ) {
				((Map)statObj).put(key, update.get(key));
			}
			applet.getDataStore().add( ar, 0L, 0L, inst.toString(), statObj );
		} catch (TException e ) {
			e.printStackTrace();
		}
	}
}
