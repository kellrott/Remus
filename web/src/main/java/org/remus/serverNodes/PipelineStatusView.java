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
import org.remus.RemusDB;
import org.remus.core.BaseNode;
import org.remus.core.DataStackInfo;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.js.JSInterface;
import org.remus.mapred.MapCallback;
import org.remus.thrift.AppletRef;

public class PipelineStatusView implements BaseNode, BaseStackNode {

	RemusPipeline pipeline;
	RemusDB datastore;
	public PipelineStatusView(RemusPipeline pipeline, RemusDB datastore) {
		this.pipeline = pipeline;
		this.datastore = datastore;
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
			for ( String appletName : pipeline.getMembers() ) {
				AppletRef arInstance = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, appletName + "/@instance" );
				for ( KeyValPair kv : datastore.listKeyPairs( arInstance ) ) {
					Map out = new HashMap();
					out.put( kv.getKey() + ":" + appletName, kv.getValue() );	
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
				for ( String appletName : pipeline.getMembers() ) {
					try {
						AppletRef arInstance = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, appletName + "/@instance" );
						for ( Object obj : datastore.get(arInstance, tmp[0]) ) {
							Map out = new HashMap();
							out.put( appletName, obj );	
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
				RemusApplet applet = pipeline.getApplet( tmp[1] );
				AppletRef arInstance = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@instance" );
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
			RemusApplet applet = pipeline.getApplet( tmp[1] );
			LinkedList<Object> out = new LinkedList<Object>();
			AppletRef arInstance = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@instance" );
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
		for ( String appletName : pipeline.getMembers() ) {
			AppletRef arInstance = new AppletRef( pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, appletName + "/@instance" );
			for ( String key : datastore.listKeys(arInstance) ) {
				list.add( key + ":" + appletName );	
			}
		}
		return list;
	}

}
