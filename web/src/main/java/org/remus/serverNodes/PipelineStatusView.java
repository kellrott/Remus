package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.core.BaseNode;
import org.remus.core.DataStackInfo;
import org.remus.core.RemusInstance;
import org.remus.fs.JSInterface;
import org.remus.mapred.MapCallback;
import org.remus.server.RemusPipelineImpl;
import org.remus.thrift.AppletRef;
import org.remus.work.RemusAppletImpl;

public class PipelineStatusView implements BaseNode, BaseStackNode {

	RemusPipelineImpl pipeline;
	public PipelineStatusView(RemusPipelineImpl pipeline) {
		this.pipeline = pipeline;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {

		if ( params.containsKey( DataStackInfo.PARAM_FLAG ) ) {
			try {
				os.write( JSON.dumps( DataStackInfo.formatInfo(PipelineStatusView.class, "status", pipeline ) ).getBytes() );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}

		if ( name.length() == 0 ) {
			for ( RemusAppletImpl applet : pipeline.getMembers() ) {
				AppletRef arInstance = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getPath() + "/@instance" );
				for ( KeyValPair kv : applet.getDataStore().listKeyPairs( arInstance ) ) {
					Map out = new HashMap();
					out.put( kv.getKey() + ":" + applet.getID(), kv.getValue() );	
					try {
						os.write( JSON.dumps( out ).getBytes() );
						os.write("\n".getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		} else {
			String [] tmp = name.split(":");
			if ( tmp.length == 1 ) {
				for ( RemusAppletImpl applet : pipeline.getMembers() ) {
					try {
						AppletRef arInstance = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getPath() + "/@instance" );
						for ( Object obj : applet.getDataStore().get(arInstance, tmp[0]) ) {
							Map out = new HashMap();
							out.put( applet.getID(), obj );	
							try {
								os.write( JSON.dumps( out ).getBytes() );
								os.write("\n".getBytes());
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					} catch (TException e) {
						e.printStackTrace();
					}
				}
			} else if ( tmp.length == 2 ) {
				RemusAppletImpl applet = pipeline.getApplet( tmp[1] );
				AppletRef arInstance = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getPath() + "/@instance" );
				try {
					for ( Object obj : applet.getDataStore().get(arInstance, tmp[0]) ) {
						Map out = new HashMap();
						out.put( applet.getID(), obj );	
						try {
							os.write( JSON.dumps( out ).getBytes() );
							os.write("\n".getBytes());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} catch (TException e) {
					e.printStackTrace();
				}
			}
		}

	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

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
				if ( len > 0 ) {
					sb.append(buffer, 0, len);
				}
			} while ( len >= 0 );

			JSInterface js = new JSInterface();
			js.init(null);
			js.initMapper(sb.toString());
			js.map( this, new MapCallback() {				
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
			}
			);


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

	@Override
	public Iterable<Object> getData(String key) {
		String [] tmp = key.split(":");
		if ( tmp.length == 2 ) {
			RemusInstance inst = new RemusInstance(tmp[0]);
			RemusAppletImpl applet = pipeline.getApplet( tmp[1] );
			LinkedList<Object> out = new LinkedList<Object>();
			AppletRef arInstance = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getPath() + "/@instance" );
			try { 
				for ( Object obj : applet.getDataStore().get(arInstance, inst.toString()) ) {
					out.add(obj);
				}
			} catch (TException e) {
				e.printStackTrace();
			}
			return out;
		}
		return null;
	}

	@Override
	public Iterable<String> getKeys() {
		LinkedList<String> list = new LinkedList<String>();
		for ( RemusAppletImpl applet : pipeline.getMembers() ) {
			AppletRef arInstance = new AppletRef( applet.getPipeline().getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getPath() + "/@instance" );
			for ( String key : applet.getDataStore().listKeys(arInstance) ) {
				list.add( key + ":" + applet.getID() );	
			}
		}
		return list;
	}

}
