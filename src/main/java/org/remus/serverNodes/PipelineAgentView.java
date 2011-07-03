package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.RemusPipeline;
import org.remus.work.RemusApplet;

public class PipelineAgentView implements BaseNode {

	RemusPipeline pipe;

	public PipelineAgentView(RemusPipeline remusPipeline) {
		this.pipe = remusPipeline;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		if ( name.length() == 0 ) {
			for ( RemusApplet applet : pipe.getMembers() ) {
				try {
					os.write( serial.dumps( applet.getID() ).getBytes() );
					os.write( "\n".getBytes() );
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
			}
		} else {
			throw new FileNotFoundException();
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
		RemusApplet applet = pipe.getApplet(name);
		if ( applet != null ) {
			return new PipelineAppletAgentView( applet );
		}
		return null;
	}

}
