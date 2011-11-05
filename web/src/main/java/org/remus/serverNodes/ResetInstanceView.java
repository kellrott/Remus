package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.RemusDatabaseException;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.NotImplemented;

public class ResetInstanceView implements BaseNode {
	RemusPipeline pipeline;

	public ResetInstanceView(RemusPipeline pipeline) {
		this.pipeline = pipeline;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		if (name.length() > 0) {
			String [] tmp = name.split(":");
			if (tmp.length == 1) {
				RemusInstance inst = pipeline.getInstance(name);
				//String subKey = pipeline.getSubKey(inst);
				//PipelineSubmission subMap = pipeline.getSubmitData(subKey);
				if (inst == null ) { // || subKey == null || subMap == null) {
					throw new FileNotFoundException();	
				}			
				try {
					pipeline.deleteInstance(inst);
				} catch (RemusDatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					os.write(inst.toString().getBytes());
					os.write(" Restarted".getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				RemusInstance inst = pipeline.getInstance(tmp[0]);
				try {
					RemusApplet applet = pipeline.getApplet(tmp[1]);
					if (inst != null && applet != null) {
						try {
							applet.deleteInstance(inst);
							//String subKey = pipeline.getSubKey(inst);
							//PipelineSubmission params = pipeline.getSubmitData(subKey);
							//BUG: Broken in new interface
							//applet.createInstance(subKey, params, inst);
						} catch (TException e) {
							e.printStackTrace();
							throw new FileNotFoundException();
						} catch (NotImplemented e) {
							e.printStackTrace();
							throw new FileNotFoundException();
						}
					}
				} catch (RemusDatabaseException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
