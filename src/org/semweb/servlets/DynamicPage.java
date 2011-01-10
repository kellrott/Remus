package org.semweb.servlets;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semweb.app.SemWebApp;


public class DynamicPage extends HttpServlet{
	/*
	 * 
	 */
	private static final long serialVersionUID = -8086862946229880731L;
	String basePath;
	String rdfStorePath;
	String baseFilePath;
	SemWebApp semApp;
	
	@Override
	public void init() throws ServletException {
		String extPath = getServletContext().getRealPath( "WEB-INF/SemWebConfig.xml" );
		semApp = new SemWebApp( new File("/opt/webapps/tcga/") );
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException{
		String path = req.getPathInfo();
		InputStream is = semApp.readPage(path);
		if ( is == null ) {
			res.sendError( HttpServletResponse.SC_NOT_FOUND );
			return;
		}
		OutputStream os = res.getOutputStream();
		byte buffer[] = new byte[1024];
		int readSize;
	 	while ( (readSize = is.read( buffer )) > -1 ) {
	 		os.write(buffer, 0, readSize);	 		
	 	}
	}
	
	/*
	private void pageRender(File semfile, String reqPath, Map paramMap, OutputStream out) throws IOException {
		InputStream is = new FileInputStream( semfile );
		PageParser parser = new PageParser(is, reqPath, paramMap, jsRunner);
		PageParser outParser = null;
		if ( parser.page.template ) {
			File templateFile = new File( baseFilePath + File.separator + "template.html" );
			InputStream tis = new FileInputStream( templateFile );
			PageParser templateParser = new PageParser(tis, "template", paramMap, jsRunner);
			Map<String,String> pageMap = new HashMap<String,String>();
			pageMap.put( "content", parser.toString() );
			templateParser.render(pageMap);			
			outParser = templateParser;
		} else {
			outParser = parser;
		}		
		int bytesRead;
		byte [] buffer = new byte[1000];
		while ((bytesRead = outParser.read(buffer)) != -1) {
			out.write(buffer, 0, bytesRead);				
		}		
	}
	*/
	
}
