package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.remus.JSON;
import org.remus.RemusDatabaseException;
import org.remus.core.RemusApplet;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;


public class AppletConfigView implements BaseNode {


	private RemusPipeline pipe;

	public AppletConfigView(RemusPipeline pipe) {
		this.pipe = pipe;	
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os)
	throws FileNotFoundException {
		
		if (name.length() == 0) {
			for (String mem : pipe.getMembers()) {
				try {
					os.write(JSON.dumps(mem).getBytes());
					os.write("\n".getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}			
		} else {
			RemusApplet applet;
			try {
				applet = pipe.getApplet(name);
			} catch (RemusDatabaseException e1) {
				throw new FileNotFoundException();
			}
			if (applet == null) {
				throw new FileNotFoundException();
			}
			try {
				os.write(JSON.dumps(applet).getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public BaseNode getChild(String name) {
		return null;
	}

}