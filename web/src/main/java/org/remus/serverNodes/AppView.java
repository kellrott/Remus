package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusDB;
import org.remus.core.BaseNode;
import org.remus.core.RemusApp;
import org.remus.core.RemusPipeline;

public class AppView implements BaseNode {

	RemusApp app;
	private HashMap<String, BaseNode> children;
	public AppView(RemusApp app, RemusDB datastore) {
		this.app = app;
		children = new HashMap<String,BaseNode>();
		children.put("@pipeline", new PipelineConfigView(app,datastore));
		children.put("@status", new ServerStatusView(app));
		children.put("@manage", new ManageApp() );
		children.put("@db", new StoreInfoView(app));
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		throw new FileNotFoundException(name);
	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os) throws FileNotFoundException {

		if ( name.length() != 0 ) {
			throw new FileNotFoundException(name);
		}

		Map out = new HashMap();
		List<String> oList = new ArrayList<String>();
		try {
		for ( String pipeName : app.getPipelines() ) {
			oList.add( pipeName );
		}
		} catch (TException e) {
			e.printStackTrace();
		}
		out.put("@", oList);
		try {
			os.write( JSON.dumps(out).getBytes() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void doPut(String name, String workerID, InputStream is, OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException(name);
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException(name);		
	}

	@Override
	public BaseNode getChild(String name) {
		if ( children.containsKey(name)) {
			return children.get(name);
		}
		if (app.hasPipeline(name)) {
			return new PipelineView(app.getPipeline(name));
		}

		return null;
	}

	public static final int GET_CALL = 1;
	public static final int DELETE_CALL = 2;
	public static final int PUT_CALL = 3;
	public static final int SUBMIT_CALL = 4;


	public void passCall( int type, String path, Map parameterMap, String workerID, InputStream inputStream, OutputStream outputStream) throws FileNotFoundException {
		String [] tmp = path.split("/");

		BaseNode curNode = this;
		Boolean called = false;
		for ( int i = 1; i < tmp.length && !called; i++ ) {		
			BaseNode next = curNode.getChild( tmp[i] );
			if ( next != null ) {
				curNode = next;
			} else {
				StringBuilder sb = new StringBuilder();
				for ( int j = i; j < tmp.length; j++) {
					if ( j != i )
						sb.append("/");
					sb.append( tmp[j] );
				}
				//System.err.println( curNode + " " + sb.toString() );
				if ( type == GET_CALL )
					curNode.doGet( sb.toString(), parameterMap, workerID, outputStream );
				if ( type == PUT_CALL )
					curNode.doPut( sb.toString(), workerID, inputStream, outputStream );
				if ( type == SUBMIT_CALL )
					curNode.doSubmit( sb.toString(), workerID, inputStream, outputStream );
				if ( type == DELETE_CALL )
					curNode.doDelete( sb.toString(), parameterMap, workerID );
				called = true;
			}
		}
		if ( !called ) {
			if ( type == GET_CALL )
				curNode.doGet( "", parameterMap, workerID, outputStream );
			if ( type == PUT_CALL )
				curNode.doPut( "", workerID, inputStream, outputStream );
			if ( type == SUBMIT_CALL )
				curNode.doSubmit( "", workerID, inputStream, outputStream );
			if ( type == DELETE_CALL )
				curNode.doDelete( "", parameterMap, workerID );
		}
	}

}
