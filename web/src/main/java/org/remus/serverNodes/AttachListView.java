package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusAttach;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class AttachListView implements BaseNode {

	RemusAttach attach;	
	String key, applet;
	RemusInstance instance;
	RemusPipeline pipeline;


	public AttachListView(RemusAttach attach, RemusPipeline pipeline, RemusInstance inst, String applet, String key) {
		this.attach = attach;
		this.instance = inst;
		this.pipeline = pipeline;
		this.key = key;
		this.applet = applet;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {

		if ( name.length() == 0 ) {
			try {
				AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), applet );
				for ( String fileName : attach.listAttachments(ar, key) ) {
					os.write( JSON.dumps(fileName).getBytes() );
					os.write("\n".getBytes());
				}
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
		} else {
			AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), applet );
			try {
				InputStream is = attach.readAttachment(ar, key, name);
				if ( is != null ) {
					byte [] buffer = new byte[10240];
					int len;
					try {
						while ( (len = is.read(buffer)) >= 0 ) {
							os.write( buffer, 0, len );
						}
						os.close();
						is.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}			
				} else {
					throw new FileNotFoundException();
				}
			} catch (NotImplemented e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public void doPut(String name, String workerID, InputStream is, OutputStream os) throws FileNotFoundException {
		if ( name.length() != 0 ) {	
			try {
				AppletRef ar = new AppletRef(pipeline.getID(), instance.toString(), applet );
				OutputStream as = attach.writeAttachment(ar, key, name);
				byte [] buffer = new byte[10240];
				int readLen;
				while ((readLen=is.read(buffer)) > 0) {
					as.write(buffer, 0, readLen);
				}
				as.close();
			} catch (IOException e) {
				e.printStackTrace();
				throw new FileNotFoundException();
			}
		} else {
			throw new FileNotFoundException();
		}
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
