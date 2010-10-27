package org.semweb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class DynamicPage extends HttpServlet{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8086862946229880731L;
	String basePath;
	JSRunner jsRunner;
	NoteInterface note;
	
	public DynamicPage() {		
		basePath = getServletContext().getRealPath( "" ) + "/WEB-INF/dynamicSystem";
		jsRunner = new JSRunner();
		note = new NoteInterface(getServletContext().getRealPath( "" ) + "/WEB-INF/rdfStore");
		jsRunner.addInterface("note", note );

	}
	
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException{
		String path = req.getPathInfo();	
		File file = new File( basePath + path );
		if ( file.exists() ) {
			
			FileInputStream is = new FileInputStream( file );
			OutputStream os = res.getOutputStream();
			byte[] buffer = new byte[1024]; // Adjust if you want
			int bytesRead;
			while ((bytesRead = is.read(buffer)) != -1) {
				os.write(buffer, 0, bytesRead);
			}
			is.close();		
		} else {
			res.sendError(HttpServletResponse.SC_NOT_FOUND);
		}		
	}
}
