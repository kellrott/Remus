package org.remus.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.remus.core.RemusApp;
import org.remus.plugin.PluginManager;
import org.remus.serverNodes.AppView;

/**
 * MasterServlet: Primary servlet interface for web based Remus Server.
 * @author kellrott
 *
 */

public class MasterServlet extends HttpServlet {
	RemusApp app;
	AppView  appView;
	//	String workDir;
	String srcDir;
	Map<String, String> configMap;

	public MasterServlet(PluginManager plugs) throws RemusDatabaseException {
		app = new RemusApp(plugs);
		appView = new AppView(app);
	}
		
	/**
	 * 
	 */
	private static final long serialVersionUID = -8067165004515233805L;


	private String getWorkerID( HttpServletRequest req ) {
		String workerID = null;
		if ( req.getCookies() != null ) {
			for ( Cookie cookie : req.getCookies() ) {
				if ( cookie.getName().compareTo( "remusWorker" ) == 0 ) {
					workerID = cookie.getValue();
				}
			}
		}
		return workerID;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {		
		String fullPath = (new File(req.getRequestURI())).getAbsolutePath();
		try {
			String workerID = getWorkerID(req);
			//if ( workerID != null ) {
			//	app.getWorkManager().touchWorkerStatus( workerID );
			//}
			OutputStream os = resp.getOutputStream();
			InputStream is = req.getInputStream();
			appView.passCall( AppView.GET_CALL, fullPath, req.getParameterMap(), workerID, is, os);
		} catch ( FileNotFoundException e ) {
			resp.sendError( HttpServletResponse.SC_NOT_FOUND );
		}		
	}



	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		
		String fullPath = (new File(req.getRequestURI())).getAbsolutePath();
		try {
			String workerID = getWorkerID(req);
			//TODO: make sure correct worker is returning assigned results before putting them in the database....
			//if ( workerID != null ) {
			//	app.getWorkManager().touchWorkerStatus( workerID );
			//}			
			InputStream is = req.getInputStream();
			OutputStream os = resp.getOutputStream();
			appView.passCall( AppView.SUBMIT_CALL, fullPath, req.getParameterMap(), workerID, is, os);
		} catch ( FileNotFoundException e ) {
			resp.sendError( HttpServletResponse.SC_NOT_FOUND );
		}	
		
		
	}



	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {		
		String fullPath = (new File(req.getRequestURI())).getAbsolutePath();
		try {
			String workerID = getWorkerID(req);
			InputStream is = req.getInputStream();
			OutputStream os = resp.getOutputStream();
			appView.passCall( AppView.PUT_CALL, fullPath, req.getParameterMap(), workerID, is, os);
		} catch ( FileNotFoundException e ) {
			resp.sendError( HttpServletResponse.SC_NOT_FOUND );
		}		
	}


	
	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {		
		String fullPath = (new File(req.getRequestURI())).getAbsolutePath();
		try {
			String workerID = getWorkerID(req);
			InputStream is = req.getInputStream();
			OutputStream os = resp.getOutputStream();
			appView.passCall( AppView.DELETE_CALL, fullPath, req.getParameterMap(), workerID, is, os);
		} catch ( FileNotFoundException e ) {
			resp.sendError( HttpServletResponse.SC_NOT_FOUND );
		}					
	}
}




