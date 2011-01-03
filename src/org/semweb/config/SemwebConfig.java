package org.semweb.config;

import java.io.File;
import java.io.IOException;
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
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class SemwebConfig {

	public class SemwebApp {
		public String name;
		public String gitPath;

		public String getName() {
			//System.out.println( "NAME:" + name );
			return name;
		}
		public String getGitPath() {
			return gitPath;
		}

	}

	public List<SemwebApp> appList;
	String workDir;

	public String getWorkDir() {
		return workDir;
	}

	public SemwebConfig (File file) {
		appList = new LinkedList<SemwebApp>();
		try {
			DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = df.newDocumentBuilder();
			Document doc = db.parse(file);			
			XPathFactory xpf = XPathFactory.newInstance();
			XPath xpath = xpf.newXPath();

			XPathExpression workdirPath = xpath.compile("//semwebConfig/workdir" );

			workDir = ((Node)workdirPath.evaluate(doc, XPathConstants.NODE)).getTextContent();


			XPathExpression semwebPath = xpath.compile("//semwebConfig/semweb" );
			XPathExpression namePath = xpath.compile("semwebName");
			XPathExpression gitPath = xpath.compile("gitpath");

			NodeList nodelist = (NodeList) semwebPath.evaluate( doc, XPathConstants.NODESET );
			for ( int i = 0; i < nodelist.getLength(); i++ ) {
				Node curNode = nodelist.item(i);
				Node nameNode = (Node) namePath.evaluate( curNode, XPathConstants.NODE );
				Node gitNode = (Node) gitPath.evaluate( curNode, XPathConstants.NODE );

				SemwebApp semweb = new SemwebApp();
				semweb.name = nameNode.getTextContent();
				semweb.gitPath =  gitNode.getTextContent();
				appList.add(semweb);
			}
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
