package org.semweb.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.semweb.config.ExtManager;
import org.semweb.config.ScriptingManager;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class PageRender {

	SemWebApp parent;
	ExtManager exts;
	ScriptingManager scriptMan;
	
	public PageRender( SemWebApp parent ) {
		this.parent = parent;
		exts = new ExtManager( parent );	
		scriptMan = new ScriptingManager( parent );			
	}

	public InputStream openPage( String path, Map paramMap ) {

		if ( path.contains(":") ) {
			String[] ps = path.split(":");
			if ( ps.length == 2 ) {
				File semfile = new File( parent.appBase, path  + PageParser.PageExt );
				try {
					DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
					DocumentBuilder db = df.newDocumentBuilder();
					Document doc = db.parse(semfile);			

					XPathFactory xpf = XPathFactory.newInstance();
					XPath xpath = xpf.newXPath();
					XPathExpression idPath = xpath.compile("//*[@id='" + ps[1] + "']");
					Element scriptNode = (Element) idPath.evaluate( doc, XPathConstants.NODE );
					
					if ( scriptNode == null ) {
						//
					}

					//PageInterface page = PageInterface.newInstance();
					/*
					jsRunner.eval(scriptNode.getTextContent(), path, page);				
					res.getWriter().print( page.toString() );
					return;
					*/
				} catch (XPathExpressionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ParserConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SAXException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		File file = new File( parent.appBase, path );
		File semfile = new File( parent.appBase, path + PageParser.PageExt );
		if ( semfile.exists() ) {
			try {
				
				PageParser parser = new PageParser( scriptMan );
				SemWebPage page = parser.parse(new FileInputStream(semfile), path );
				InputStream os = page.render(paramMap);
				return os;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			if ( file.isDirectory() ) {
				File indexFile = new File( semfile.toString(), "index.html" + PageParser.PageExt );
				if ( indexFile.exists() ) {
					//pageRender( indexFile, path, paramMap, res.getOutputStream() );
				} else {

				}
			} else {
				//fileServlet.doGet(req, res);			
			}
		}	
		return null;
	}
}
