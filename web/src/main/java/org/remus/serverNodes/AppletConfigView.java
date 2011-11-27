package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusDatabaseException;
import org.remus.core.PipelineDesc;
import org.remus.core.RemusApplet;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.NotImplemented;


public class AppletConfigView implements BaseNode {


	private RemusPipeline pipe;

	public AppletConfigView(RemusPipeline pipe) {
		this.pipe = pipe;	
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
			throws FileNotFoundException {
		try {
			RemusApplet applet = pipe.getApplet(name);
			if (applet != null) {
				pipe.deleteApplet(applet);
			}
		} catch (TException e) {
			throw new FileNotFoundException();
		} catch (RemusDatabaseException e) {
			throw new FileNotFoundException();
		} catch (NotImplemented e) {
			throw new FileNotFoundException();
		}
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
			String [] tmp = name.split("/");
			if (tmp.length == 1) {
				try {
					RemusApplet applet = pipe.getApplet(name);
					if (applet == null) {
						throw new FileNotFoundException();
					}
					try {
						os.write(JSON.dumps(applet).getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (RemusDatabaseException e1) {
					throw new FileNotFoundException();
				}
			} else {
				try {
					RemusApplet applet = pipe.getApplet(tmp[0]);
					if (applet == null) {
						throw new FileNotFoundException();
					}
					try {
						InputStream is = applet.readAttachment(tmp[1]);
						byte [] buffer = new byte[10240];
						int len;
						while ((len=is.read(buffer))> 0) {
							os.write(buffer,0,len);
						}
						is.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (NotImplemented e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (RemusDatabaseException e1) {
					throw new FileNotFoundException();
				}
			}
		}

	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		try {
			StringBuilder sb = new StringBuilder();
			byte [] buffer = new byte[1024];
			int len;
			while ((len = is.read(buffer)) > 0) {
				sb.append(new String(buffer, 0, len));
			}
			System.err.println(sb.toString());
			Object data = JSON.loads(sb.toString());
			pipe.putApplet(name, data);
			//app.putPipeline(name, new PipelineDesc(data));
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
