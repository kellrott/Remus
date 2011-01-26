package org.remus;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class RemusParser {


	public static List<CodeFragment> parse(InputStream is, String pagePath) {

		try {
			DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = df.newDocumentBuilder();
			Document doc = db.parse(is);			


			XPathFactory xpf = XPathFactory.newInstance();
			XPath xpath = xpf.newXPath();
			
			HashMap<Integer, XPathExpression> codeTypes = new HashMap<Integer, XPathExpression>();
			codeTypes.put( CodeFragment.MAPPER,  xpath.compile("//*/remus_mapper") );
			codeTypes.put( CodeFragment.SPLITTER,xpath.compile("//*/remus_splitter"));
			codeTypes.put( CodeFragment.REDUCER, xpath.compile("//*/remus_reducer"));
			codeTypes.put( CodeFragment.MERGER,  xpath.compile("//*/remus_merger"));
			codeTypes.put( CodeFragment.PIPE,    xpath.compile("//*/remus_pipe"));
			List<CodeFragment> outList = new LinkedList<CodeFragment>();			
			for ( Integer codeType : codeTypes.keySet() ) {
				NodeList nodes = (NodeList) codeTypes.get( codeType ).evaluate( doc, XPathConstants.NODESET );
				for ( int i = 0; i < nodes.getLength(); i++ ) {
					NamedNodeMap attr = nodes.item(i).getAttributes();
					String id = pagePath + ":" + attr.getNamedItem("id").getTextContent();
					System.out.println(id);
					outList.add( new CodeFragment("python", nodes.item(i).getTextContent(), id, codeType) ); 
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
	public static void main(String [] args) throws FileNotFoundException {
		RemusParser p = new RemusParser();
		for ( CodeFragment code : p.parse(new FileInputStream(new File(args[0])), args[0]) ) {
			System.out.println( code );
		}
	}
}
