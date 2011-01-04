package org.semweb.servlets;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.semweb.app.PageParser;
import org.semweb.app.PageManager;
import org.semweb.app.SemWebApp;
import org.semweb.config.ExtManager;
import org.semweb.plugins.PageInterface;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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
		rdfStorePath = getServletConfig().getInitParameter("rdfStorePath");
		baseFilePath = getServletConfig().getInitParameter("basePath");
		String extPath = getServletContext().getRealPath( "WEB-INF/SemWebConfig.xml" );
		semApp = new SemWebApp( new File("/opt/webapps/tcga/") );
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException{
		String path = req.getPathInfo();
		InputStream is = semApp.readPage(path);
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
