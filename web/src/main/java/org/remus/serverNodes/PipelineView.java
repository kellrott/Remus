package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusWeb;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;

/**
 * The web node that handles the pipeline page:
 * ie. http://<host>/<pipeline> .
 * @author kellrott
 *
 */
public class PipelineView implements BaseNode {

	private static final int BLOCK_SIZE=2048;
	HashMap<String, BaseNode> children;

	RemusWeb web;
	RemusPipeline pipe;
	RemusDB datastore;
	RemusAttach attachstore;

	public PipelineView(RemusPipeline pipe, RemusWeb web) {
		this.web = web;
		this.pipe = pipe;
		this.datastore = web.getDataStore();
		this.attachstore = web.getAttachStore();
		children = new HashMap<String, BaseNode>();
		children.put("@submit", new SubmitView(pipe, datastore, web));
		children.put("@status", new PipelineStatusView(pipe, web));
		children.put("@instance", new PipelineInstanceListViewer(pipe, datastore));

		children.put("@pipeline", new AppletConfigView(pipe));

		children.put("@error", new PipelineErrorView(pipe));
		children.put("@reset", new ResetInstanceView(pipe));

		children.put("@attach", new PipelineAttachmentList(pipe));
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		//Deletions should be done through one of the sub-views, or in a parent view
		throw new FileNotFoundException();
	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os) throws FileNotFoundException {
		if (name.length() > 0) {
			throw new FileNotFoundException();
		}
		Map out = new HashMap();
		AppletRef ar = new AppletRef(pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, Constants.PIPELINE_APPLET);
		for (String subKey : pipe.getSubmits()) {
			try {
				os.write(JSON.dumps(subKey).getBytes());
				os.write("\n".getBytes());
				
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}

	}

	@Override
	public void doPut(String name, String workerID, InputStream is, OutputStream os) throws FileNotFoundException {
		if (name.contains("/")) {
			throw new FileNotFoundException();
		}
		if (pipe != null) {
			try {
				StringBuilder sb = new StringBuilder();
				byte [] buffer = new byte[BLOCK_SIZE];
				int len;
				while ((len = is.read(buffer)) > 0) {
					sb.append(new String(buffer, 0, len));
				}
				System.err.println(sb.toString());
				Object data = JSON.loads(sb.toString());
				pipe.putApplet(pipe, name, data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	/**
	 * A node to handle generic attachment requests for a pipeline:
	 * ie. http://<host>/<pipeline>/@attach
	 * These are the static attachments that come as part of the pipeline.
	 * @author kellrott
	 *
	 */
	class PipelineAttachmentList implements BaseNode {
		RemusPipeline pipeline;
		PipelineAttachmentList(RemusPipeline pipeline) {
			this.pipeline = pipeline;
		}

		@Override
		public void doDelete(String name, Map params, String workerID) throws FileNotFoundException { }

		@Override
		public void doGet(String name, Map params, String workerID,
				OutputStream os)
		throws FileNotFoundException {
			try {
				InputStream ais = pipe.readAttachment(name);
				if (ais != null) {
					byte [] buffer = new byte[BLOCK_SIZE];
					int len;
					try {
						while ((len = ais.read(buffer)) >= 0) {
							os.write(buffer, 0, len);
						}
						os.close();
						ais.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}			
				} else {
					throw new FileNotFoundException();
				}		
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void doPut(String name, String workerID, InputStream is,
				OutputStream os) throws FileNotFoundException {
			try {
				OutputStream aos = pipe.writeAttachment(name);
				byte [] buffer = new byte[BLOCK_SIZE];
				int len;
				while ((len = is.read(buffer)) >= 0) {
					aos.write(buffer, 0, len);
				}
				aos.close();
				is.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}

		@Override
		public void doSubmit(String name, String workerID, InputStream is,
				OutputStream os) throws FileNotFoundException {}

		@Override
		public BaseNode getChild(String name) {
			return null;
		}
	}

	/**
	 * A node to handle requests to specific file attachments.
	 * @author kellrott
	 *
	 */
	class PipelineAttachmentFile implements BaseNode {
		RemusPipeline pipeline;
		String fileName;
		PipelineAttachmentFile(RemusPipeline pipeline, String fileName) {
			this.fileName = fileName;
			this.pipeline = pipeline;
		}

		@Override
		public void doDelete(String name, Map params, String workerID) throws FileNotFoundException { }

		@Override
		public void doGet(String name, Map params, String workerID,
				OutputStream os)
		throws FileNotFoundException {
			try {
				InputStream fis = pipe.readAttachment(fileName);
				if (fis != null) {
					byte [] buffer = new byte[BLOCK_SIZE];
					int len;
					try {
						while ((len = fis.read(buffer)) >= 0) {
							os.write(buffer, 0, len);
						}
						os.close();
						fis.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}			
				} else {
					throw new FileNotFoundException();
				}		
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void doPut(String name, String workerID, InputStream is,
				OutputStream os) throws FileNotFoundException {}

		@Override
		public void doSubmit(String name, String workerID, InputStream is,
				OutputStream os) throws FileNotFoundException {}

		@Override
		public BaseNode getChild(String name) {
			return null;
		}
	}


	@Override
	public BaseNode getChild(String name) {
		if (children.containsKey(name)) {
			return children.get(name);
		}

		try {
			RemusInstance inst = RemusInstance.getInstance(datastore, pipe.getID(), name);
			if (inst != null) {
				return new PipelineInstanceView(pipe, inst, web);				
			}
		} catch (TException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try { 
			if (pipe.hasAttachment(name)) {
				return new PipelineAttachmentFile(pipe, name);
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}


}
