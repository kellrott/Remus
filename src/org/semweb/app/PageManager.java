package org.semweb.app;

import java.io.ByteArrayInputStream;
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
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.SAXException;

public class PageManager {

	SemWebApp parent;
	ExtManager exts;
	ScriptingManager scriptMan;

	public PageManager( SemWebApp parent ) {
		this.parent = parent;
		exts = new ExtManager( parent );	
		scriptMan = new ScriptingManager( parent );			
	}

	public PageRequest openPage( String path, Map<String,String> paramMap ) {

		if ( path.contains(":") ) {
			String[] ps = path.split(":");
			if ( ps.length == 2 ) {
				File semfile = new File( parent.appBase, ps[0]  + PageParser.PageExt );
				if ( semfile.exists() ) {
					try {
						DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
						DocumentBuilder db = df.newDocumentBuilder();
						Document doc = db.parse(semfile);			

						XPathFactory xpf = XPathFactory.newInstance();
						XPath xpath = xpf.newXPath();
						XPathExpression idPath = xpath.compile("//*[@id='" + ps[1] + "']");
						Element scriptNode = (Element) idPath.evaluate( doc, XPathConstants.NODE );

						DOMImplementationLS domImplLS = (DOMImplementationLS) doc.getImplementation();
						LSSerializer serializer = domImplLS.createLSSerializer();
						String scriptStr = serializer.writeToString(scriptNode);
						
						PageParser parser = new PageParser( this );
						SemWebPage page = parser.parse(new ByteArrayInputStream(scriptStr.getBytes()), path );
						
						PageRequest request = new PageRequest(this, semfile, path, PageRequest.DYNAMIC, page);
						return request;
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
		}

		File file = new File( parent.appBase, path );
		File semfile = new File( parent.appBase, path + PageParser.PageExt );
		if ( semfile.exists() ) {
			try{		
				PageParser parser = new PageParser( this );
				SemWebPage page = parser.parse(new FileInputStream(semfile), path );
				PageRequest request = new PageRequest(this, semfile, path, PageRequest.DYNAMIC, page);
				return request;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			if ( file.isDirectory() ) {
				File indexFile = new File( semfile.toString(), "index.html" + PageParser.PageExt );
				if ( indexFile.exists() ) {
					return new PageRequest(this, indexFile, path, PageRequest.DYNAMIC, null);
				} else {
					
				}
			} else {
				if ( file.exists() ) {
					return new PageRequest(this, file, path, PageRequest.STATIC, null);
				}
			}
		}	
		return null;
	}
}
