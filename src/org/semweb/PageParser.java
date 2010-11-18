package org.semweb;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.*;

//import org.mozilla.javascript.WrapFactory;
import org.xml.sax.Attributes;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import org.antlr.stringtemplate.*;


public class PageParser extends InputStream {

	static String PageExt = ".semweb";
	
	public class PageHandler extends DefaultHandler {
		Pattern codeRE = Pattern.compile("\\&\\{(.*?)\\}\\&", Pattern.DOTALL );		
		StringBuilder curBuffer = null;
		boolean serverCode = false;
		@Override
		public void processingInstruction(String target, String data)
		throws SAXException {
			if ( target.compareTo("js") == 0 ) {
				page.reset();
				evalCode(data, page);
				XMLOutput( page.toString() );				
			}
		}

		@Override
		public void startDocument() throws SAXException {
			curBuffer = new StringBuilder();
		}

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {
			if ( qName.compareTo("script") == 0 ) {
				if ( attributes.getValue("type").compareTo( "server/javascript" ) == 0 ) {
					serverCode = true;
				}				
			}
			if ( !serverCode ) {
				XMLOutput(curBuffer.toString());
				XMLOutput( "<" + qName );
				for ( int i = 0; i < attributes.getLength(); i++ ) {
					String name = attributes.getLocalName(i);
					String value = attributes.getValue(i);
					if ( name.startsWith("eval:") ) {
						XMLOutput( " " + name.substring(5) + "='" + evalCode(value, null)  + "'" );

					} else {
						XMLOutput( " " + name + "='" + value + "'" );
					}
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
				serverCode = false;
			}
		}

		@Override
		public void characters(char[] ch, int start, int length)
		throws SAXException {
			//System.err.println( new String(ch, start, length) );
			if ( !serverCode ) {
				curBuffer.append(ch, start, length );
			}
		}
	}


	public class BlankResolver implements EntityResolver {
		public InputSource resolveEntity (String publicId, String systemId) {
			return null;
		}
	} 

	public String evalCode(String code, PageInterface page) {
		if ( code.length() > 0 )
			return codeRunner.eval( code, curPageName, page );	
		return "";
	}

	private void XMLOutput( String str ) {
		stringBuiler.append( str );
	}

	PageInterface page;

	StringBuilder stringBuiler;
	JSRunner codeRunner;
	String curPageName;
	String outString = null;
	ByteArrayInputStream outStream = null;
	StringTemplate st = null;

	public PageParser(InputStream is, String pageName, Map paramMap, JSRunner codeRunner ) {		
		this.codeRunner = codeRunner;

		curPageName = pageName;
		stringBuiler = new StringBuilder();
		try {
			page = PageInterface.newInstance();
			page.args = paramMap;
			codeRunner.extManager.setDataSource("page", page );

			XMLReader parser = XMLReaderFactory.createXMLReader();

			//SAXParserFactory sax = SAXParserFactory.newInstance();
			//SAXParser parser;
			//parser = sax.newSAXParser();
			DefaultHandler ph= new PageHandler();

			parser.setContentHandler(ph);
			parser.setEntityResolver( new BlankResolver() );

			parser.parse( new InputSource(is) );
			//System.out.println(stringBuiler.toString());
			st = new StringTemplate(stringBuiler.toString());
			st.setAttribute("page", page);
			for ( String key : page.paramMap.keySet() ) {
				//System.err.println(key);
				st.setAttribute(key, page.paramMap.get(key) );
			}			
			//} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			//	e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



	public void render() {
		render(null);		
	}

	public void render(Map<String,String> content) {
		if ( content != null) {
			for ( String name : content.keySet() ) {
				st.setAttribute(name, content.get(name));
			}
		}
		outString = st.toString();
		outStream = new ByteArrayInputStream( outString.getBytes() );
	}

	@Override
	public String toString() {
		if ( outString == null )
			render();
		return outString;
	}


	@Override
	public int read() throws IOException {
		if ( outStream == null )
			render();
		return outStream.read();
	}

}
