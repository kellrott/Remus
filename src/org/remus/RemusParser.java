package org.remus;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.remus.applet.RemusApplet;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class RemusParser {
	RemusApp parent;
	public RemusParser( RemusApp app ) {
		parent = app;
	}

	public List<RemusApplet> parse(InputStream is, String pagePath) {

		try {
			DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = df.newDocumentBuilder();
			Document doc = db.parse(is);			


			XPathFactory xpf = XPathFactory.newInstance();
			XPath xpath = xpf.newXPath();

			HashMap<Integer, XPathExpression> codeTypes = new HashMap<Integer, XPathExpression>();
			codeTypes.put( RemusApplet.MAPPER,  xpath.compile("//*/remus_mapper") );
			codeTypes.put( RemusApplet.SPLITTER,xpath.compile("//*/remus_splitter"));
			codeTypes.put( RemusApplet.REDUCER, xpath.compile("//*/remus_reducer"));
			codeTypes.put( RemusApplet.MERGER,  xpath.compile("//*/remus_merger"));
			codeTypes.put( RemusApplet.PIPE,    xpath.compile("//*/remus_pipe"));
			List<RemusApplet> outList = new LinkedList<RemusApplet>();			
			for ( Integer appletType : codeTypes.keySet() ) {
				NodeList nodes = (NodeList) codeTypes.get( appletType ).evaluate( doc, XPathConstants.NODESET );
				for ( int i = 0; i < nodes.getLength(); i++ ) {
					NamedNodeMap attr = nodes.item(i).getAttributes();
					String id = pagePath + ":" + attr.getNamedItem("id").getTextContent();
					//System.out.println(id);

					CodeFragment cf =  new CodeFragment("python", nodes.item(i).getTextContent());
					RemusApplet applet = RemusApplet.newApplet(id, cf, appletType);
					
					if ( attr.getNamedItem("input") != null ) {
						String inputStr = attr.getNamedItem("input").getTextContent();
						for ( String inName : inputStr.split(",") ) {
							RemusPath iRef = new RemusPath(parent, inName, pagePath );
							applet.addInput(iRef);
						}
					}
					if ( attr.getNamedItem("output") != null ) {
						String outputStr = attr.getNamedItem("output").getTextContent();
						for ( String outName : outputStr.split(",") ) {
							applet.addOutput(outName);
						}
					}
					if ( appletType == RemusApplet.MERGER ) {
						String lInputStr = attr.getNamedItem("left").getTextContent();
						RemusPath lIRef = new RemusPath(parent, lInputStr, pagePath );
						applet.addLeftInput(lIRef);						
						String rInputStr = attr.getNamedItem("right").getTextContent();
						RemusPath rIRef = new RemusPath(parent, rInputStr, pagePath );
						applet.addRightInput(rIRef);						
					}
					
					outList.add( applet ); 
				}
			}			
			return outList;

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
		return null;
	}


	/*
	public static void main(String [] args) throws FileNotFoundException {
		RemusParser p = new RemusParser();
		for ( CodeFragment code : p.parse(new FileInputStream(new File(args[0])), args[0]) ) {
			System.out.println( code );
		}
	}
	*/
}
