package org.semweb.config;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.semweb.datasource.DataSource;
import org.semweb.datasource.InitException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class ExtManager {

	
	Map<String, DataSource> dsList;
	
	public ExtManager(File file) {
		dsList = new HashMap<String, DataSource>();
		try {
			DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = df.newDocumentBuilder();
			Document doc = db.parse(file);			
			XPathFactory xpf = XPathFactory.newInstance();
			XPath xpath = xpf.newXPath();

			XPathExpression dsPath = xpath.compile("//extConfig/dataSource" );
			XPathExpression classPath = xpath.compile("className");
			XPathExpression namePath = xpath.compile("plugName");
			XPathExpression configPath = xpath.compile("config");
			
			NodeList nodelist = (NodeList) dsPath.evaluate( doc, XPathConstants.NODESET );

			for ( int i = 0; i < nodelist.getLength(); i++ ) {
				Node curNode = nodelist.item(i);
				String className = classPath.evaluate(curNode);
				String plugName = namePath.evaluate(curNode);
				try {
					System.out.println("LOADING: " + className );
					System.out.flush();
					Class<?> c = Class.forName(className);
					DataSource ds = (DataSource) c.newInstance();
					Node configNode = (Node) configPath.evaluate( curNode, XPathConstants.NODE );
					ConfigMap config = ConfigMap.XmlNodeParse(configNode);
					ds.init(config);
					dsList.put( plugName , ds);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InitException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
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

	public Set<String>  getDataSourceNames() {
		return dsList.keySet();
	}
	
	public DataSource getDataSource(String name) {
		return dsList.get(name);
	}

	public void setDataSource(String name, DataSource ds) {
		dsList.put(name, ds);
	}
	
	static public void main(String [] args) {
		new ExtManager(new File( "WebContent/WEB-INF/extConfig.xml" ) );
	}

}
