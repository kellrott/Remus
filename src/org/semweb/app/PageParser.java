package org.semweb.app;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.semweb.app.SemWebPage.CodeFragment;
import org.semweb.config.ScriptingManager;
import org.semweb.scripting.ScriptingInterface;
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
		String curLang = null, curID=null;
		StringBuilder stringBuiler;
		Map<String,CodeFragment> codeMap;
		PageHandler() {
			stringBuiler = new StringBuilder();
			codeMap = new HashMap<String,CodeFragment>();
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
				if ( attributes.getValue("lang") != null && attributes.getValue("id") != null) {
					if ( scriptMan.hasLang( attributes.getValue("lang") ) ) {
						serverCode = true;
						curLang =  attributes.getValue("lang");
						curID = attributes.getValue("id");
						curBuffer = new StringBuilder();
					}
				}				
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
				curBuffer = new StringBuilder();
				XMLOutput("</" + qName + ">");
			} else {
				addCode(curID, curLang, curBuffer.toString());
				serverCode = false;
			}
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
		
		
		public void addCode(String id, String lang, String source) {
			if ( source.length() > 0 ) {
				//ScriptingInterface si = scriptMan.getLang(lang);
				CodeFragment code = new CodeFragment();
				code.lang = lang;
				code.source = source;
				codeMap.put(id, code);
			}
		}

		public SemWebPage getPage() {
			SemWebPage page = new SemWebPage(stringBuiler.toString(), codeMap);
			return page;
		}
		
	}


	public class BlankResolver implements EntityResolver {
		public InputSource resolveEntity (String publicId, String systemId) {
			return null;
		}
	} 
	
	ScriptingManager scriptMan;

	public PageParser( ScriptingManager scriptManager ) {
		this.scriptMan = scriptManager;		
	}
	
	public SemWebPage parse(InputStream is, String pageName) {		
		try {
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
