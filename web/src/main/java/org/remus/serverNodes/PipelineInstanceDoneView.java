package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.remus.RemusDatabaseException;
import org.remus.RemusWeb;
import org.remus.core.AppletInstance;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;

public class PipelineInstanceDoneView implements BaseNode {

	private RemusPipeline pipeline;
	private RemusInstance inst;
	private RemusWeb web;
	
	public PipelineInstanceDoneView(RemusWeb web, RemusPipeline pipeline,
			RemusInstance inst) {
		this.web = web;
		this.pipeline = pipeline;
		this.inst = inst;
	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os)
			throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		if (name.length() > 0) {
			try {
				AppletInstance applet = pipeline.getAppletInstance(inst, name);
				if (applet != null) {
					if (applet.getRecord().isStore()) {
						applet.setComplete();
					}
				}
			} catch (RemusDatabaseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
			throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

}
