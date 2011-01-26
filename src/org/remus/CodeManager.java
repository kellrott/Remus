package org.remus;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;

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

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class CodeManager {

	RemusApp  parent;
	//PluginManager plugMan;

	public CodeManager( RemusApp parent ) {
		this.parent = parent;
		//plugMan = new PluginManager( parent );	
	}

	/*
	public PageRequest openPage( String requestPath ) {
		String path = (new File(parent.srcbase, requestPath)).getAbsolutePath();
		if ( path.contains(":") ) {
			String[] ps = path.split(":");
			if ( ps.length == 2 ) {
				File semfile = new File( ps[0]  + RemusParser.PageExt );
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
						
						//return request;
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
		return null;
	}
	*/
}
