package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.langs.JSInterface;
import org.remus.mapred.MapCallback;
import org.remus.server.RemusPipelineImpl;
import org.remus.work.RemusAppletImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineInstanceQueryView implements BaseNode {

	RemusPipelineImpl pipeline;
	RemusInstance inst;
	private Logger logger;
	public PipelineInstanceQueryView(RemusPipelineImpl pipeline, RemusInstance inst) {
		this.pipeline = pipeline;
		this.inst = inst;
		logger = LoggerFactory.getLogger(PipelineInstanceQueryView.class);

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, final Serializer serial,
			final InputStream is, final OutputStream os) throws FileNotFoundException {

		RemusAppletImpl applet = pipeline.getApplet(name);
		if ( applet == null ) {
			throw new FileNotFoundException();
		}

		try { 
			char [] buffer = new char[1024];
			int len;
			StringBuilder sb = new StringBuilder();
			Reader in = new InputStreamReader(is);
			do {
				len = in.read(buffer);
				if ( len > 0 ) {
					sb.append(buffer, 0, len);
				}
			} while ( len >= 0 );

			AppletInstanceView appletView = new AppletInstanceView(applet, inst);

			JSInterface js = new JSInterface();
			js.init(null);
			js.initMapper(sb.toString());
			js.map( appletView, new MapCallback() {				
				@Override
				public void emit(String key, Object val) {
					Map out = new HashMap();
					out.put(key, val);		
					try {
						os.write(serial.dumps(out).getBytes());
						os.write("\n".getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
					);
		} catch (IOException e) {
			logger.debug( e.toString() );
			e.printStackTrace();
		}
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
			throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

}
