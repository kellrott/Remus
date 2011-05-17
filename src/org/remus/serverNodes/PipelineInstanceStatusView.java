package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.work.RemusApplet;

public class PipelineInstanceStatusView implements BaseNode {

	RemusPipeline pipeline;
	RemusInstance inst;
	public PipelineInstanceStatusView(RemusPipeline pipeline, RemusInstance inst) {
		this.pipeline = pipeline;
		this.inst = inst;
	}


	@Override
	public void doDelete(String name, Map params, String workerID) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		try {

			if ( name.length() == 0 ) {
				for ( RemusApplet applet : pipeline.getMembers() ) {
					for ( Object data : applet.getDataStore().get( applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, inst.toString() ) ) {
						Map out = new HashMap();
						out.put(applet.getID(), data);
						os.write( serial.dumps(out).getBytes() );
						os.write("\n".getBytes() );
					}
				}
			} else {
				RemusApplet applet = pipeline.getApplet( name );
				if ( applet != null ) {
					for ( Object data : applet.getDataStore().get( applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, inst.toString() ) ) {
						Map out = new HashMap();
						out.put(applet.getID(), data);
						os.write( serial.dumps(out).getBytes() );
						os.write("\n".getBytes() );
					}
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
