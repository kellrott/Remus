package org.remus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.serverNodes.BaseNode;
import org.remus.work.AppletInstanceView;
import org.remus.work.InstanceErrorView;
import org.remus.work.InstanceStatusView;
import org.remus.work.RemusApplet;

public class PipelineInstanceViewer implements BaseNode {

	RemusPipeline pipeline;
	RemusInstance inst;

	public PipelineInstanceViewer( RemusPipeline pipeline, RemusInstance inst ) {
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
		if ( name.length() == 0)  {
			for ( RemusApplet applet : pipeline.getMembers() ) {
				for ( Object instObj : applet.getDataStore().get(applet.getPath() + InstanceStatusView.InstanceStatusName, RemusInstance.STATIC_INSTANCE_STR, inst.toString() ) ) {
					Map out = new HashMap();
					out.put( applet.getID(), instObj );	
					try {
						os.write( serial.dumps( out ).getBytes() );
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
		RemusApplet applet = pipeline.getApplet(name);
		if ( applet != null ) {
			return new AppletInstanceView(applet, inst);
		}
		
		if ( name.compareTo("@error") == 0 ) {
			return new InstanceErrorView(pipeline, inst);
		}
		
		return null;		
	}

}
