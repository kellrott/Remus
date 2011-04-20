package org.remus.work;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.mpstore.Serializer;
import org.remus.RemusInstance;
import org.remus.serverNodes.BaseNode;

public class InstanceStatusView implements BaseNode {

	RemusApplet applet;
	public InstanceStatusView(RemusApplet applet) {
		this.applet = applet;
	}

	public void setWorkStat(RemusInstance inst, int doneCount, int errorCount, int totalCount) {
		Map u = new HashMap();
		u.put("_doneCount", doneCount);
		u.put("_errorCount", errorCount);
		u.put("_totalCount", totalCount);
		updateStatus(inst, u);
	}

	public void updateStatus( RemusInstance inst, Map update ) {
		Object statObj = null;
		for ( Object obj : applet.datastore.get( applet.getPath() + "/@status" , RemusInstance.STATIC_INSTANCE_STR, inst.toString()) ) {
			statObj = obj;
		}
		if ( statObj == null ) {
			statObj = new HashMap();
		}
		for ( Object key : update.keySet() ) {
			((Map)statObj).put(key, update.get(key));
		}
		applet.datastore.add( applet.getPath() + "/@status", RemusInstance.STATIC_INSTANCE_STR, 0L, 0L, inst.toString(), statObj );
	}
	
	@Override
	public void doDelete(Map params) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {

		if ( name.length() == 0 ) {
			for ( KeyValuePair kv : applet.datastore.listKeyPairs( applet.getPath() + "/@status" , RemusInstance.STATIC_INSTANCE_STR ) ) {			
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
			for ( Object obj : applet.datastore.get( applet.getPath() , RemusInstance.STATIC_INSTANCE_STR, name) ) {
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
			InputStream is, OutputStream os) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}



}
