package org.semweb.app;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.semweb.config.PluginManager;
import org.semweb.config.TemplateManager;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class PageManager {

	SemWebApp parent;
	PluginManager plugMan;
	TemplateManager templateMan;

	public PageManager( SemWebApp parent ) {
		this.parent = parent;
		plugMan = new PluginManager( parent );	
		templateMan = new TemplateManager( parent );
	}

	public PageRequest openWebPage( String path ) {
		return openPage( (new File(parent.appBase, path)).getAbsolutePath() );
	}
	
	public PageRequest openPage( String path ) {

		if ( path.contains(":") ) {
			String[] ps = path.split(":");
			if ( ps.length == 2 ) {
				File semfile = new File( ps[0]  + PageParser.PageExt );
				if ( semfile.exists() ) {
					try {
						DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
						DocumentBuilder db = df.newDocumentBuilder();
						Document doc = db.parse(semfile);			

						XPathFactory xpf = XPathFactory.newInstance();
						XPath xpath = xpf.newXPath();
						XPathExpression idPath = xpath.compile("//*[@id='" + ps[1] + "']");
						Element scriptNode = (Element) idPath.evaluate( doc, XPathConstants.NODE );
						if ( scriptNode == null)
							return null;
										
						TransformerFactory transFactory = TransformerFactory.newInstance();
						Transformer transformer = transFactory.newTransformer();
						StringWriter buffer = new StringWriter();
						transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
						transformer.transform(new DOMSource(scriptNode),
						      new StreamResult(buffer));
						String scriptStr = buffer.toString();
						
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
					} catch (TransformerConfigurationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (TransformerException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

		File file = new File( path );
		File semfile = new File( path + PageParser.PageExt );
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
