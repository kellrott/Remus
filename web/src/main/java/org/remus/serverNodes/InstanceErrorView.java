package org.remus.serverNodes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusDatabaseException;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class InstanceErrorView implements BaseNode {


	public class AppletInstanceErrorView implements BaseNode {
		RemusPipeline pipeline;
		RemusApplet applet;
		RemusInstance inst;
		public AppletInstanceErrorView( RemusPipeline pipeline, RemusApplet applet, RemusInstance inst ) {
			this.pipeline = pipeline;
			this.applet = applet;
			this.inst = inst;			
		}
		@Override
		public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
			throw new FileNotFoundException();
		}

		@Override
		public void doGet(String name, Map params, String workerID,
				OutputStream os)
		throws FileNotFoundException {

			AppletRef ar = new AppletRef( pipeline.getID(), inst.toString(), applet.getID() + "/@error" );
			for ( KeyValPair kv : applet.getDataStore().listKeyPairs( ar ) ) {
				Map out = new HashMap();
				out.put(kv.getKey(), kv.getValue());
				try {
					os.write( JSON.dumps(out).getBytes());
					os.write("\n".getBytes());
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

			/*
			StringBuilder sb = new StringBuilder();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String curline = null;
			try {
				while ((curline = br.readLine()) != null  ) {
					sb.append(curline);
				}

				Object data = serial.loads( sb.toString() );
				for (Object key : ((Map)data).keySet() ) {
					applet.getPipeline().getApp().getWorkManager().errorWork( 
							workerID, applet, inst, Integer.parseInt(key.toString()), 
							((Map)data).get(key).toString() );
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 */

		}

		@Override
		public BaseNode getChild(String name) {
			// TODO Auto-generated method stub
			return null;
		}

	}

	RemusPipeline pipeline;
	RemusInstance inst;
	public InstanceErrorView(RemusPipeline pipeline, RemusInstance inst) {
		this.pipeline = pipeline;
		this.inst = inst;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		for ( String appletName : pipeline.getMembers() ) {
			try {
				RemusApplet applet = pipeline.getApplet(appletName);
				applet.deleteErrors(inst);
			} catch (TException e) {
				throw new FileNotFoundException();
			} catch (NotImplemented e) {
				throw new FileNotFoundException();
			} catch (RemusDatabaseException e) {
				throw new FileNotFoundException();
			}
		}
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException(); 
	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		try {
			RemusApplet applet = pipeline.getApplet(name);
			if ( applet != null ) {
				return new AppletInstanceErrorView( pipeline, applet, inst );
			}
		} catch (RemusDatabaseException e) {
			e.printStackTrace();
		}
		return null;
	}

}
