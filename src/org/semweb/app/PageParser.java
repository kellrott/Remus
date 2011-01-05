package org.semweb.app;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


import org.semweb.config.ScriptingManager;
import org.semweb.template.TemplateInterface;
import org.xml.sax.Attributes;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

public class PageParser {

	static String PageExt = ".semweb";

	public class PageHandler extends DefaultHandler {
		Pattern codeRE = Pattern.compile("\\&\\{(.*?)\\}\\&", Pattern.DOTALL );		
		StringBuilder curBuffer = null;
		boolean serverCode = false;
		String curLang = null, curID=null, curHREF=null;
		StringBuilder stringBuiler;
		CodeFragment curCode;
		InputConnection curInput;
		Map<String,SemWebApplet> codeMap;
		PageHandler() {
			stringBuiler = new StringBuilder();
			codeMap = new HashMap<String,SemWebApplet>();
		}

		/*
		@Override
		public void processingInstruction(String target, String data)
		throws SAXException {
			if ( target.compareTo("js") == 0 ) {
				page.reset();
				evalCode(data, page);
				XMLOutput( page.toString() );				
			}
		}
		 */

		@Override
		public void startDocument() throws SAXException {
			curBuffer = new StringBuilder();
		}

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			if ( qName.compareTo("semweb") == 0 ) {
				XMLOutput(curBuffer.toString());
				curBuffer = new StringBuilder();

				serverCode = true;
				curID = attributes.getValue("id");
			}
			if ( !serverCode ) {
				XMLOutput(curBuffer.toString());
				XMLOutput( "<" + qName );
				for ( int i = 0; i < attributes.getLength(); i++ ) {
					String name = attributes.getLocalName(i);
					String value = attributes.getValue(i);
					XMLOutput( " " + name + "='" + value + "'" );
				}
				XMLOutput(">");
				curBuffer = new StringBuilder();
			} else {
				if ( qName.compareTo("input") == 0) {
					if ( attributes.getValue("lang") != null ) {
						if ( parent.scriptMan.hasLang( attributes.getValue("lang") ) ) {
							curLang = attributes.getValue("lang");
						}
					} else if ( attributes.getValue("href") != null ) {
						curHREF = attributes.getValue("href");
					}
				}
				if ( qName.compareTo("mapper") == 0 || qName.compareTo("writer") == 0) {
					if ( attributes.getValue("lang") != null ) {
						if ( parent.scriptMan.hasLang( attributes.getValue("lang") ) ) {
							curLang = attributes.getValue("lang");
						}
					}
				}
				curBuffer = new StringBuilder();
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName)
		throws SAXException {
			if ( ! serverCode ) {
				XMLOutput(curBuffer.toString());
				XMLOutput("</" + qName + ">");
			} else {
				if ( qName.compareTo("input") == 0 ) {
					curInput = new InputConnection();
					curInput.inputHREF = curHREF;
					curInput.inputLang = curLang;
					curInput.inputSource = curBuffer.toString();
				} else if ( qName.compareTo("mapper") == 0 ) {
					curCode = new CodeFragment(curLang, curBuffer.toString(), CodeFragment.MAPPER);
				} else if ( qName.compareTo("writer") == 0 ) {
					curCode = new CodeFragment(curLang, curBuffer.toString(), CodeFragment.WRITER);
				} else if ( qName.compareTo("semweb") == 0 ) {
					addApplet(curID, curInput, curCode );
					TemplateInterface ti = parent.templateMan.getTemplateInterface();
					XMLOutput( ti.replaceCode(curID) );
					serverCode = false;
					curInput = null;
					curCode = null;
				}
			}
			curBuffer = new StringBuilder();
		}

		@Override
		public void characters(char[] ch, int start, int length)
		throws SAXException {
			//System.err.println( new String(ch, start, length) );
			//if ( !serverCode ) {
			curBuffer.append(ch, start, length );
			//}
		}

		private void XMLOutput( String str ) {
			stringBuiler.append( str );
		}


		public void addApplet(String id, InputConnection conn, CodeFragment code) {
			SemWebApplet applet = new SemWebApplet();
			applet.code = code;
			applet.input = conn;
			codeMap.put(id, applet);
		}

		public SemWebPage getPage() {
			SemWebPage page = new SemWebPage(parent, pageName, stringBuiler.toString(), codeMap);
			return page;
		}

	}


	public class BlankResolver implements EntityResolver {
		public InputSource resolveEntity (String publicId, String systemId) {
			return null;
		}
	} 

	PageManager parent;
	String pageName;
	public PageParser( PageManager parent ) {
		this.parent = parent;		
	}

	public SemWebPage parse(InputStream is, String pageName) {		
		try {
			this.pageName = pageName;
			XMLReader parser = XMLReaderFactory.createXMLReader();
			PageHandler ph= new PageHandler();
			parser.setContentHandler(ph);
			parser.setEntityResolver( new BlankResolver() );
			parser.parse( new InputSource(is) );
			return ph.getPage();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
