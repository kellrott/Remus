package org.remus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mpstore.JsonSerializer;
import org.mpstore.Serializer;

/**
 * MasterServlet: Primary servlet interface for web based Remus Server.
 * @author kellrott
 *
 */

public class MasterServlet extends HttpServlet {
	RemusApp app;
	Serializer serializer;
	//	String workDir;
	String srcDir;
	Map<String, String> configMap;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		try {
			configMap = new HashMap<String, String>();			
			Enumeration names = config.getInitParameterNames();
			while ( names.hasMoreElements() ) {
				String name = (String) names.nextElement();
				configMap.put(name, config.getInitParameter(name));
			}
			serializer = new JsonSerializer();
			app = new RemusApp(configMap);
		} catch (RemusDatabaseException e) {
			throw new ServletException(e.toString());
		}		
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
			app.passCall( RemusApp.GET_CALL, fullPath, req.getParameterMap(), workerID, serializer, is, os);
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
			app.passCall( RemusApp.SUBMIT_CALL, fullPath, req.getParameterMap(), workerID, serializer, is, os);
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
			app.passCall( RemusApp.PUT_CALL, fullPath, req.getParameterMap(), workerID, serializer, is, os);
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
			app.passCall( RemusApp.DELETE_CALL, fullPath, req.getParameterMap(), workerID, serializer, is, os);
		} catch ( FileNotFoundException e ) {
			resp.sendError( HttpServletResponse.SC_NOT_FOUND );
		}					
	}
}




