package org.semweb;

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

import org.semweb.config.ExtManager;
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
	JSRunner jsRunner;

	String rdfStorePath;
	String baseFilePath;

	FileServlet fileServlet;
	
	@Override
	public void init() throws ServletException {
		rdfStorePath = getServletConfig().getInitParameter("rdfStorePath");
		baseFilePath = getServletConfig().getInitParameter("basePath");
		String extPath = getServletContext().getRealPath( "WEB-INF/extConfig.xml" );
		ExtManager ext = new ExtManager(new File(extPath ));
		jsRunner = new JSRunner(ext);
		
		
		fileServlet = new FileServlet();		
		fileServlet.init(this.getServletConfig());		
		fileServlet.init();
	}
	
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException{
		String path = req.getPathInfo();
		
		if ( path.contains(":") ) {
			String[] ps = path.split(":");
			if ( ps.length == 2 ) {
			File semfile = new File( baseFilePath + File.separator + ps[0] + PageParser.PageExt );
			try {
				DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
				DocumentBuilder db = df.newDocumentBuilder();
				Document doc = db.parse(semfile);			

				XPathFactory xpf = XPathFactory.newInstance();
				XPath xpath = xpf.newXPath();
				XPathExpression idPath = xpath.compile("//*[@id='" + ps[1] + "']");
				Element scriptNode = (Element) idPath.evaluate( doc, XPathConstants.NODE );
				if ( scriptNode == null ) {
					res.sendError(HttpServletResponse.SC_NOT_FOUND);
					return;
				}
				
				PageInterface page = PageInterface.newInstance();
				jsRunner.eval(scriptNode.getTextContent(), path, page);				
				res.getWriter().print( page.toString() );
				return;
			} catch (XPathExpressionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
		}
		
		File file = new File( baseFilePath + File.separator + path );
		File semfile = new File( baseFilePath + File.separator + path + PageParser.PageExt );
		
		if ( semfile.exists() ) {
				pageRender( semfile, path, req.getParameterMap(), res.getOutputStream() );
		} else {
			if ( file.isDirectory() ) {
				File indexFile = new File( semfile.toString() + File.separator + "index.html" + PageParser.PageExt );
				if ( indexFile.exists() ) {
					pageRender( indexFile, path, req.getParameterMap(), res.getOutputStream() );
				} else {

				}
			} else {
				fileServlet.doGet(req, res);			
			}
		}		
	}
	
	
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
	
}
