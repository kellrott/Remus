package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusDatabaseException;
import org.remus.RemusWeb;
import org.remus.core.AppletInstance;
import org.remus.core.BaseStackNode;
import org.remus.core.DataStackNode;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.mapred.MapReduceCallback;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.WorkMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineInstanceQueryView implements BaseNode {

	private static final int OUT_JSON = 0;
	private static final int OUT_TSV = 1;

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
		if (name.length() == 0) {
			AppletRef ar = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, Constants.INSTANCE_APPLET);
			for (String key : web.getDataStore().listKeys(ar)) {
				if (key.startsWith(inst.toString() + ":")) {
					try {
						os.write(JSON.dumps(key).getBytes());
						os.write("\n".getBytes());
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		} else {
			int mode = OUT_JSON;
			AppletRef ar = new AppletRef(pipeline.getID(), inst.toString(), name);
			int limit = 1000;
			if (params.containsKey("limit")) {
				String limitCount = ((String []) params.get("limit"))[0];
				limit = Integer.parseInt(limitCount);
			}
			if (params.containsKey("tsv")) {
				mode = OUT_TSV;
			}
			String start = "";
			if (params.containsKey("start")) {
				start = ((String []) params.get("start"))[0];
			}


			try {
				if (mode == OUT_JSON) {
					jsonDump(os, web.getDataStore().keyValSlice(ar, start, limit));
				}
				if (mode == OUT_TSV) {
					tsvDump(os, web.getDataStore().keyValSlice(ar, start, limit));
				}
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private void tsvDump(OutputStream os, List<KeyValPair> kvList) {
		Set<String> colSet = new HashSet<String>();
		for (KeyValPair kv : kvList) {
			Object val = kv.getValue();
			if (val instanceof Map) {
				for (Object col : ((Map)val).keySet()) {
					colSet.add(col.toString());
				}
			}
		}

		try { 
			StringBuilder sb = new StringBuilder();
			sb.append("KEY");
			Object []cols = colSet.toArray();
			for (Object col : cols) { 
				sb.append("\t");
				sb.append(col.toString());
			}
			sb.append("\n");
			os.write(sb.toString().getBytes());
			for (KeyValPair kv : kvList) {
				Object val = kv.getValue();
				sb = new StringBuilder();
				sb.append(kv.getKey());
				for (Object col : cols) {
					sb.append("\t");
					if (val instanceof Map) {
						sb.append(((Map)val).get(col));						
					}
				}
				sb.append("\n");
				os.write(sb.toString().getBytes());				
			}
		} catch (IOException e) {

		}
	}

	private void jsonDump(OutputStream os, List<KeyValPair> kvList) {
		for (KeyValPair kv : kvList) {
			Map out = new HashMap();
			out.put(kv.getKey(), kv.getValue());
			try {
				os.write(JSON.dumps(out).getBytes());
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
			
			AppletRef ar = new AppletRef(pipeline.getID(), inst.toString(), applet.getID());
			DataStackNode ds = new DataStackNode(web.getDataStore(), ar);
			
			final List<KeyValPair> outList = new LinkedList<KeyValPair>();
			web.jsRequest( sb.toString(), WorkMode.MAP, ds, new BaseStackNode() {
				
				@Override
				public List<String> keySlice(String keyStart, int count) {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public List<String> getValueJSON(String key) {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public void delete(String key) {
					// TODO Auto-generated method stub
					
				}
				
				@Override
				public boolean containsKey(String key) {
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public void add(String key, String data) {
					outList.add(new KeyValPair(key, JSON.loads(data), 0, 0));
				}
			}); 
			
			jsonDump(os, outList);
			
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
