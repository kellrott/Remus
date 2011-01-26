package org.remus;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class RemusParser {


	public List<CodeFragment> parse(InputStream is, String pagePath) {

		try {
			DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = df.newDocumentBuilder();
			Document doc = db.parse(is);			


			XPathFactory xpf = XPathFactory.newInstance();
			XPath xpath = xpf.newXPath();
			XPathExpression mapperPath = xpath.compile("//*/remus_mapper");
			XPathExpression splitterPath = xpath.compile("//*/remus_splitter");
			XPathExpression reducerPath = xpath.compile("//*/remus_reducer");
			XPathExpression mergerPath = xpath.compile("//*/remus_merger");
			XPathExpression pipePath = xpath.compile("//*/remus_pipe");

			List<CodeFragment> outList = new LinkedList<CodeFragment>();
			
			NodeList mapperNodes = (NodeList) mapperPath.evaluate( doc, XPathConstants.NODESET );
			for ( int i = 0; i < mapperNodes.getLength(); i++ ) {
				outList.add( new CodeFragment("python",  mapperNodes.item(i).getTextContent(), CodeFragment.MAPPER) ); 
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
		p.parse(new FileInputStream(new File(args[0])), args[0]);
	}
}
