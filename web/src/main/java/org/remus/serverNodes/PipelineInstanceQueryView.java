package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.remus.JSON;
import org.remus.RemusDatabaseException;
import org.remus.RemusWeb;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.mapred.MapReduceCallback;
import org.remus.server.BaseNode;
import org.remus.thrift.WorkMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineInstanceQueryView implements BaseNode {

	RemusWeb web;
	RemusPipeline pipeline;
	RemusInstance inst;
	private Logger logger;
	public PipelineInstanceQueryView(RemusWeb web,
			RemusPipeline pipeline, RemusInstance inst) {
		this.web = web;
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
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, final InputStream is,
			final OutputStream os) throws FileNotFoundException {

		try { 
			RemusApplet applet = pipeline.getApplet(name);
			if (applet == null) {
				throw new FileNotFoundException();
			}
			char [] buffer = new char[1024];
			int len;
			StringBuilder sb = new StringBuilder();
			Reader in = new InputStreamReader(is);
			do {
				len = in.read(buffer);
				if (len > 0) {
					sb.append(buffer, 0, len);
				}
			} while (len >= 0);

			AppletInstanceView appletView = new AppletInstanceView(pipeline, applet, inst);
			/*
			web.jsRequest( sb.toString(), WorkMode.MAP, appletView, 
					new MapReduceCallback(null, null, null, null, null) {
				@Override
				public void emit(String key, Object val) {
					Map out = new HashMap();
					out.put(key, val);		
					try {
						os.write(JSON.dumps(out).getBytes());
						os.write("\n".getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
			*/
		} catch (IOException e) {
			logger.debug(e.toString());
			e.printStackTrace();
		} catch (RemusDatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

}
