package org.semweb.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.semweb.template.TemplateInterface;
import org.xml.sax.Attributes;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

public class PageParser {

	public static String PageExt = ".semweb";

	public class PageHandler extends DefaultHandler {
		StringBuilder curBuffer = null;
		boolean serverCode = false;
		String curLang = null, curID=null;
		StringBuilder stringBuiler;
		CodeFragment curCode;
		PageReference curInput;
		Map<String,SemWebApplet> codeMap;
		
		Map<String,Integer> semwebTags;
		
		PageHandler() {
			semwebTags = new HashMap<String,Integer>();
			semwebTags.put("semweb_writer", CodeFragment.WRITER);
			semwebTags.put("semweb_splitter", CodeFragment.SPLITTER);
			semwebTags.put("semweb_mapper", CodeFragment.MAPPER );
			semwebTags.put("semweb_reducer", CodeFragment.REDUCER );
			
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
			if ( semwebTags.containsKey(qName ) ) {
				XMLOutput(curBuffer.toString());
				curBuffer = new StringBuilder();
				serverCode = true;
				curID = attributes.getValue("id");
				
				if ( qName.compareTo("semweb_writer") == 0) {
					if ( attributes.getValue("type") != null ) {
						if ( parent.plugMan.hasPlugin( attributes.getValue("type") ) ) {
							curLang = attributes.getValue("type");
						}
					}
					if ( attributes.getValue("input") != null ) {
							try {
								curInput = new PageReference( parent, attributes.getValue("input"), new File(parent.parent.getPageBase(), pagePath) );
							} catch (FileNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						
					}
				}
				/*
				if ( qName.compareTo("mapper") == 0 || qName.compareTo("writer") == 0) {
					if ( attributes.getValue("lang") != null ) {
						if ( parent.scriptMan.hasLang( attributes.getValue("lang") ) ) {
							curLang = attributes.getValue("lang");
						}
					}
				}
				*/
				curBuffer = new StringBuilder();
				
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
			} 
		}

		@Override
		public void endElement(String uri, String localName, String qName)
		throws SAXException {
			if ( ! serverCode ) {
				XMLOutput(curBuffer.toString());
				XMLOutput("</" + qName + ">");
			} else {
				if ( semwebTags.containsKey( qName ) ) {
					curCode = new CodeFragment(curLang, curBuffer.toString(), semwebTags.get(qName) );
					addApplet(curID, curInput, curCode );
					TemplateInterface ti = parent.templateMan.getTemplateInterface();
					XMLOutput( ti.replaceCode(curID) );
					serverCode = false;
					curInput = null;
					curCode = null;
					serverCode = false;
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


		public void addApplet(String id, PageReference conn, CodeFragment code) {
			
			try {
				SemWebApplet applet = new SemWebApplet( new PageReference(parent, ":" + id, new File(parent.parent.appBase, pagePath)), conn, code);
				codeMap.put(id, applet);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public SemWebPage getPage() {
			SemWebPage page = new SemWebPage(parent, pagePath, stringBuiler.toString(), codeMap);
			return page;
		}

	}


	public class BlankResolver implements EntityResolver {
		public InputSource resolveEntity (String publicId, String systemId) {
			return null;
		}
	} 

	PageManager parent;
	String pagePath;
	public PageParser( PageManager parent ) {
		this.parent = parent;		
	}

	public SemWebPage parse(InputStream is, String pagePath) {		
		try {
			this.pagePath = pagePath;
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
