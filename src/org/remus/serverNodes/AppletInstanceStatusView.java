package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.mpstore.Serializer;
import org.remus.RemusInstance;
import org.remus.work.RemusApplet;

public class AppletInstanceStatusView implements BaseNode {

	public static final String InstanceStatusName = "/@instance";
	
	RemusApplet applet;
	public AppletInstanceStatusView(RemusApplet applet) {
		this.applet = applet;
	}


	public Object getStatus(RemusInstance inst) {
		Object statObj = null;
		for ( Object obj : applet.getDataStore().get( applet.getPath() + InstanceStatusName , RemusInstance.STATIC_INSTANCE_STR, inst.toString()) ) {
			statObj = obj;
		}
		return statObj;
	}
	
	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {

		if ( name.length() == 0 ) {
			for ( KeyValuePair kv : applet.getDataStore().listKeyPairs( applet.getPath() + InstanceStatusName , RemusInstance.STATIC_INSTANCE_STR ) ) {			
				Map out = new HashMap();
				out.put( kv.getKey(), kv.getValue() );	
				try {
					os.write( serial.dumps( out ).getBytes() );
					os.write("\n".getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}		
		} else {
			for ( Object obj : applet.getDataStore().get( applet.getPath() + InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR, name) ) {
				Map out = new HashMap();
				out.put(name, obj );				
				try {
					os.write( serial.dumps(out).getBytes() );
					os.write("\n".getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void updateStatus(RemusApplet applet,
			RemusInstance inst, Map update) {
		Object statObj = null;
		for ( Object obj : applet.getDataStore().get( applet.getPath() + InstanceStatusName , RemusInstance.STATIC_INSTANCE_STR, inst.toString()) ) {
			statObj = obj;
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		for ( Object key : update.keySet() ) {
			((Map)statObj).put(key, update.get(key));
		}
		applet.getDataStore().add( applet.getPath() + InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR, 0L, 0L, inst.toString(), statObj );
	}
}
