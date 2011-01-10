package org.semweb.work;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.semweb.app.PageManager;
import org.semweb.app.PageRequest;
import org.semweb.app.SemWebApp;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class WorkManager {
	SemWebApp parent;
	File workdir;
	public WorkManager(SemWebApp app, File workdir) {
		this.parent = app;	
		this.workdir = workdir;
	}


	public void scanWork() {
		scanWork( parent.getPageBase() );
	}

	public void scanWork(File file) {
		if ( file.isDirectory() ) {
			for ( File child : file.listFiles() ) {
				scanWork(child);
			}
		} else if ( file.getName().endsWith(".semweb") ) {
			try {
				DocumentBuilderFactory df = DocumentBuilderFactory.newInstance();
				DocumentBuilder db = df.newDocumentBuilder();
				Document doc = db.parse(file);			

				XPathFactory xpf = XPathFactory.newInstance();
				XPath xpath = xpf.newXPath();

				XPathExpression writerPath = xpath.compile("//*/semweb_writer");
				NodeList writerNodes = (NodeList) writerPath.evaluate( doc, XPathConstants.NODESET );
				if ( writerNodes != null) {
					for ( int i = 0; i < writerNodes.getLength(); i++ ) {
						Element curWriter = (Element)writerNodes.item(i);
						System.out.println( "WRITER:" + curWriter.getAttribute("id") );
						PageManager pageMan = parent.getPageManager();
						PageRequest req = pageMan.openPage( file.getAbsolutePath() );
						System.out.println( req );
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
	}
	
	
	public static void main(String []args) {
		SemWebApp app = new SemWebApp( new File(args[0]) );
		WorkManager wm = new WorkManager(app, new File(args[1]));
		wm.scanWork();
	}



}
