package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.RemusInstance;
import org.remus.work.RemusApplet;

public class PipelineAppletAgentView implements BaseNode {

	RemusApplet applet;
	public PipelineAppletAgentView(RemusApplet applet) {
		this.applet = applet;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		
		if ( params.containsKey(DataStackInfo.PARAM_FLAG) ) {
			try {
				os.write( serial.dumps( DataStackInfo.formatInfo("status", applet.getPipeline()) ).getBytes() );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
		
		if ( name.length() == 0 ) {
			for ( String key : applet.getDataStore().listKeys(applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR ) ) {
				try {
					os.write( serial.dumps( key + ":" + applet.getID() ).getBytes() );
					os.write( "\n".getBytes() );
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
			}
		} else {
			String tmp[] = name.split(":");
			if ( tmp.length == 2 && tmp[1].compareTo( applet.getID() ) == 0 ) {
				for ( Object obj : applet.getDataStore().get( applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, tmp[0] ) ) {
					Map out = new HashMap();
					out.put( name, obj );				
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

}
