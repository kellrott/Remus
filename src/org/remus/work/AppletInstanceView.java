package org.remus.work;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpstore.KeyValuePair;
import org.mpstore.Serializer;
import org.remus.RemusInstance;
import org.remus.serverNodes.BaseNode;

public class AppletInstanceView implements BaseNode {

	RemusApplet applet;
	RemusInstance inst;

	public AppletInstanceView(RemusApplet applet, RemusInstance inst) {
		this.applet = applet;
		this.inst = inst;
	}

	@Override
	public void doDelete(Map params) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID, Serializer serial,
			OutputStream os) throws FileNotFoundException {

		String sliceStr = null;
		int sliceSize = 0;
		if ( params.containsKey("slice") ) {
			sliceStr = ((String [])params.get("slice"))[0];
			sliceSize = Integer.parseInt(sliceStr);
		}

		if ( name.length() == 0 ) {
			if ( sliceStr == null ) {
				for ( KeyValuePair kv : applet.datastore.listKeyPairs( applet.getPath() , inst.toString() ) ) {			
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
				for ( String sliceKey : applet.datastore.keySlice( applet.getPath(), inst.toString(), "", sliceSize) ) {
					for ( Object value : applet.datastore.get(  applet.getPath(), inst.toString(), sliceKey ) ) {
						Map oMap = new HashMap();
						oMap.put( sliceKey, value);
						try {
							os.write( serial.dumps( oMap ).getBytes() );
							os.write("\n".getBytes());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

			}
		} else {
			if ( sliceStr == null ) {
				for ( Object obj : applet.datastore.get( applet.getPath() , inst.toString(), name) ) {
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
			} else {
				for ( String sliceKey : applet.datastore.keySlice( applet.getPath(), inst.toString(), name, sliceSize) ) {
					for ( Object value : applet.datastore.get(  applet.getPath(), inst.toString(), sliceKey ) ) {
						Map oMap = new HashMap();
						oMap.put( sliceKey, value);
						try {
							os.write( serial.dumps( oMap ).getBytes() );
							os.write("\n".getBytes());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}

	}

	@Override
	public void doPut(String name, String workerID, Serializer serial, InputStream is, OutputStream os) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {

		try {
			Set outSet = new HashSet<Integer>();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String curline = null;
			List<KeyValuePair> inputList = new ArrayList<KeyValuePair>();
			while ( (curline = br.readLine() ) != null ) {
				Map inObj = (Map)serial.loads(curline);	
				long jobID = Long.parseLong( inObj.get("id").toString() );
				outSet.add((int)jobID);
				inputList.add( new KeyValuePair( jobID, 
						(Long)inObj.get("order"), (String)inObj.get("key") , 
						inObj.get("value") ) );
			}
			applet.datastore.add( applet.getPath(), 
					inst.toString(),
					inputList );		

		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
