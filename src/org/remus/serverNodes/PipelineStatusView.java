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

import org.mpstore.KeyValuePair;
import org.mpstore.Serializer;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.langs.JSInterface;
import org.remus.mapred.MapCallback;
import org.remus.work.RemusApplet;

public class PipelineStatusView implements BaseNode, BaseStackNode {

	RemusPipeline pipeline;
	public PipelineStatusView(RemusPipeline pipeline) {
		this.pipeline = pipeline;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		
		if ( params.containsKey( DataStackInfo.PARAM_FLAG ) ) {
			try {
				os.write( serial.dumps( DataStackInfo.formatInfo("status", pipeline ) ).getBytes() );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}
		
		if ( name.length() == 0 ) {
			for ( RemusApplet applet : pipeline.getMembers() ) {
				for ( KeyValuePair kv : applet.getDataStore().listKeyPairs(applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR) ) {
					Map out = new HashMap();
					out.put( kv.getKey() + ":" + applet.getID(), kv.getValue() );	
					try {
						os.write( serial.dumps( out ).getBytes() );
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
				for ( RemusApplet applet : pipeline.getMembers() ) {
					for ( Object obj : applet.getDataStore().get(applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, tmp[0]) ) {
						Map out = new HashMap();
						out.put( applet.getID(), obj );	
						try {
							os.write( serial.dumps( out ).getBytes() );
							os.write("\n".getBytes());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			} else if ( tmp.length == 2 ) {
				RemusApplet applet = pipeline.getApplet( tmp[1] );
				for ( Object obj : applet.getDataStore().get(applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, tmp[0]) ) {
					Map out = new HashMap();
					out.put( applet.getID(), obj );	
					try {
						os.write( serial.dumps( out ).getBytes() );
						os.write("\n".getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, final Serializer serial,
			InputStream is, final OutputStream os) throws FileNotFoundException {		
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
			for ( Object obj : applet.getDataStore().get(applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, inst.toString()) ) {
				out.add(obj);
			}
			return out;
		}
		return null;
	}

	@Override
	public Iterable<String> getKeys() {
		LinkedList<String> list = new LinkedList<String>();
		for ( RemusApplet applet : pipeline.getMembers() ) {
			for ( String key : applet.getDataStore().listKeys(applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR) ) {
				list.add( key + ":" + applet.getID() );	
			}
		}
		return list;
	}

}
