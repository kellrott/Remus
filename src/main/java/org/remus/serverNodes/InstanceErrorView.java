package org.remus.serverNodes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.mpstore.Serializer;
import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.server.RemusPipelineImpl;
import org.remus.work.RemusAppletImpl;

public class InstanceErrorView implements BaseNode {


	public class AppletInstanceErrorView implements BaseNode {
		RemusAppletImpl applet;
		RemusInstance inst;
		public AppletInstanceErrorView( RemusAppletImpl applet, RemusInstance inst ) {
			this.applet = applet;
			this.inst = inst;			
		}
		@Override
		public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
			throw new FileNotFoundException();
		}

		@Override
		public void doGet(String name, Map params, String workerID,
				Serializer serial, OutputStream os)
						throws FileNotFoundException {

			for ( KeyValuePair kv : applet.getDataStore().listKeyPairs(applet.getPath() + "/@error", inst.toString() ) ) {
				Map out = new HashMap();
				out.put(kv.getKey(), kv.getValue());
				try {
					os.write(serial.dumps(out).getBytes());
					os.write("\n".getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
		}

		@Override
		public void doPut(String name, String workerID, Serializer serial,
				InputStream is, OutputStream os) throws FileNotFoundException {
			throw new FileNotFoundException();
		}

		@Override
		public void doSubmit(String name, String workerID, Serializer serial,
				InputStream is, OutputStream os) throws FileNotFoundException {
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

	RemusPipelineImpl pipeline;
	RemusInstance inst;
	public InstanceErrorView(RemusPipelineImpl pipeline, RemusInstance inst) {
		this.pipeline = pipeline;
		this.inst = inst;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		for ( RemusAppletImpl applet : pipeline.getMembers() ) {
			applet.deleteErrors(inst);
		}
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException(); 
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
		RemusAppletImpl applet = pipeline.getApplet(name);
		if ( applet != null ) {
			return new AppletInstanceErrorView( applet, inst );
		}
		return null;
	}

}
