package org.semweb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semweb.config.ExtManager;

public class DynamicPage extends HttpServlet{
	/*
	 * 
	 */
	private static final long serialVersionUID = -8086862946229880731L;
	String basePath;
	JSRunner jsRunner;

	String rdfStorePath;
	String dynamicFilePath;

	@Override
	public void init() throws ServletException {
		rdfStorePath = getServletConfig().getInitParameter("rdfStorePath");
		dynamicFilePath = getServletConfig().getInitParameter("dynamicFilePath");
		String extPath = getServletContext().getRealPath( "WEB-INF/extConfig.xml" );
		ExtManager ext = new ExtManager(new File(extPath ));
		jsRunner = new JSRunner(ext);
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException{
		String path = req.getPathInfo();	
		File file = new File( dynamicFilePath + File.separator + path );
		File templateFile = new File( dynamicFilePath + File.separator + "template.html" );
		if ( file.exists() ) {
		
			InputStream is = new FileInputStream( file );
			PageParser parser = new PageParser(is, path, req.getParameterMap(), jsRunner);

			PageParser outParser = null;
			if ( parser.page.template ) {
				InputStream tis = new FileInputStream( templateFile );
				PageParser templateParser = new PageParser(tis, "template", req.getParameterMap(), jsRunner);
				Map<String,String> pageMap = new HashMap<String,String>();
				pageMap.put( "content", parser.toString() );
				templateParser.render(pageMap);			
				outParser = templateParser;
			} else {
				outParser = parser;
			}
			
			OutputStream out = res.getOutputStream();			
			int bytesRead;
			byte [] buffer = new byte[1000];
			while ((bytesRead = outParser.read(buffer)) != -1) {
				out.write(buffer, 0, bytesRead);				
			}
			
		} else {
			res.sendError(HttpServletResponse.SC_NOT_FOUND);
		}		
	}
}
