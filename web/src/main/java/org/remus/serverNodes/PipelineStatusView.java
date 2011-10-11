package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.RemusWeb;
import org.remus.core.AppletInstanceStack;
import org.remus.core.BaseStackNode;
import org.remus.core.DataStackInfo;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.mapred.MapReduceCallback;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.WorkMode;

public class PipelineStatusView implements BaseNode {

	RemusWeb web;
	RemusPipeline pipeline;
	RemusDB datastore;
	public PipelineStatusView(RemusPipeline pipeline, RemusWeb web) {
		this.web = web;
		this.pipeline = pipeline;
		this.datastore = web.getDataStore();
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {

		if (params.containsKey(DataStackInfo.PARAM_FLAG)) {
			try {
				os.write(JSON.dumps(DataStackInfo.formatInfo(PipelineStatusView.class, "status", pipeline)).getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}

		if (name.length() == 0) {
			for (String appletName : pipeline.getMembers()) {
				AppletRef arInstance = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, appletName + Constants.INSTANCE_APPLET);
				for (KeyValPair kv : datastore.listKeyPairs(arInstance)) {
					Map out = new HashMap();
					out.put(kv.getKey() + ":" + appletName, kv.getValue());
					try {
						os.write(JSON.dumps(out).getBytes());
						os.write("\n".getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		} else {
			String [] tmp = name.split(":");
			if (tmp.length == 1) {
				for (String appletName : pipeline.getMembers()) {
					try {
						AppletRef arInstance = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, appletName + Constants.INSTANCE_APPLET);
						for (Object obj : datastore.get(arInstance, tmp[0])) {
							Map out = new HashMap();
							out.put(appletName, obj);
							try {
								os.write(JSON.dumps(out).getBytes());
								os.write("\n".getBytes());
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					} catch (TException e) {
						e.printStackTrace();
					} catch (NotImplemented e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} else if (tmp.length == 2) {
				try {
					RemusApplet applet = pipeline.getApplet(tmp[1]);
					AppletRef arInstance = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + Constants.INSTANCE_APPLET);
					for (Object obj : applet.getDataStore().get(arInstance, tmp[0])) {
						Map out = new HashMap();
						out.put(applet.getID(), obj);	
						try {
							os.write(JSON.dumps(out).getBytes());
							os.write("\n".getBytes());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} catch (TException e) {
					e.printStackTrace();
				} catch (NotImplemented e) {
					e.printStackTrace();
				} catch (RemusDatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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
			final OutputStream os) throws FileNotFoundException {		
		try {
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
			AppletInstanceStack ai =  new AppletInstanceStack(web.getDataStore(), web.getAttachStore(), pipeline.getID()) {
				@Override
				public void add(String key, String data) {
					// TODO Auto-generated method stub
					Map out = new HashMap();
					out.put(key, JSON.loads(data));
					try {
						os.write(JSON.dumps(out).getBytes());
						os.write("\n".getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			};			
			web.jsRequest(sb.toString(), WorkMode.MAP, ai, ai);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
