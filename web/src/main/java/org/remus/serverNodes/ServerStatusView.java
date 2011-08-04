package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.remus.JSON;
import org.remus.core.BaseNode;
import org.remus.core.RemusApp;
import org.remus.work.RemusAppletImpl;

public class ServerStatusView implements BaseNode {

	RemusApp app;
	public ServerStatusView(RemusApp app) {
		this.app = app;
	}

	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {
		try {
			Map outMap = new HashMap();				
			Map workerMap = new HashMap();
			/*
			for ( String wID : app.getWorkManager().getWorkers()) {
				//TODO: put in more methods to access work manager statistics
				Map curMap = new HashMap();
				curMap.put("_activeCount", app.getWorkManager().getWorkerActiveCount(wID) );
				Date lastDate = app.getWorkManager().getLastAccess(wID);
				if ( lastDate != null )
					curMap.put("_lastContact", System.currentTimeMillis() - lastDate.getTime()  );
				workerMap.put(wID, curMap );	
			}
			Map<RemusApplet, Integer> assignMap = app.getWorkManager().getAssignRateMap();
			Map aMap = new HashMap();
			for ( RemusApplet applet : assignMap.keySet() ) {
				aMap.put(applet.getPath(), assignMap.get(applet) );
			}
			outMap.put( "_assignRate", aMap );
			outMap.put( "_workers", workerMap );
			outMap.put( "_workBufferSize", app.getWorkManager().getWorkBufferSize() );
			//outMap.put("finishRate", workManage.getFinishRate() );
			 
			 */
			os.write( JSON.dumps(outMap).getBytes() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
