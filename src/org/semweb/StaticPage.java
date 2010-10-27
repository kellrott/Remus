package org.semweb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class StaticPage extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4327048299783011752L;

	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException{
		String basePath = getServletContext().getRealPath( "" ) + "/WEB-INF/fileSystem";
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
