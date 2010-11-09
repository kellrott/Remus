package org.semweb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class DynamicPage extends HttpServlet{
	/*
	 * 
	 */
	private static final long serialVersionUID = -8086862946229880731L;
	String basePath;
	JSRunner jsRunner;
	NoteInterface note;

	String rdfStorePath;
	String dynamicFilePath;

	@Override
	public void init() throws ServletException {
		rdfStorePath = getServletConfig().getInitParameter("rdfStorePath");
		dynamicFilePath = getServletConfig().getInitParameter("dynamicFilePath");
		jsRunner = new JSRunner();
		note = new NoteInterface( rdfStorePath );
		jsRunner.addInterface("note", note );

	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException{
		String path = req.getPathInfo();	
		File file = new File( dynamicFilePath + File.separator + path );
		if ( file.exists() ) {
			InputStream is = new FileInputStream( file );
			PageParse parser = new PageParse(is, path, jsRunner);
			res.getWriter().print( parser );
			/*
			StringBuilder sb = new StringBuilder();
			int bytesRead;
			byte [] buffer = new byte[1000];
			while ((bytesRead = is.read(buffer)) != -1) {
				sb.append( new String( buffer, 0, bytesRead) );
			}
			res.getWriter().print( jsRunner.eval(sb.toString(), path, false ) );
			*/			
		} else {
			res.sendError(HttpServletResponse.SC_NOT_FOUND);
		}		
	}
}
