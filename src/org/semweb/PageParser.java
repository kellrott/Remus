package org.semweb;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import javax.xml.parsers.*;

import org.mozilla.javascript.WrapFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import org.antlr.stringtemplate.*;


public class PageParser extends InputStream {

	public class PageHandler extends DefaultHandler {
		Pattern codeRE = Pattern.compile("\\&\\{(.*?)\\}\\&", Pattern.DOTALL );		
		StringBuilder curBuffer = null;
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

		@Override
		public void endElement(String uri, String localName, String qName)
		throws SAXException {			
			XMLOutput(curBuffer.toString());
			curBuffer = new StringBuilder();
			XMLOutput("</" + qName + ">");
		}

		@Override
		public void characters(char[] ch, int start, int length)
		throws SAXException {
			//System.err.println( new String(ch, start, length) );
			curBuffer.append(ch, start, length );
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
	String outString;
	ByteArrayInputStream outStream;

	public PageParser(InputStream is, String pageName, JSRunner codeRunner ) {		
		this.codeRunner = codeRunner;	
		curPageName = pageName;
		stringBuiler = new StringBuilder();
		try {
			page = PageInterface.newInstance();
			SAXParserFactory sax = SAXParserFactory.newInstance();
			SAXParser parser;
			parser = sax.newSAXParser();
			DefaultHandler ph= new PageHandler();
			parser.parse(is, ph );
			//System.out.println(stringBuiler.toString());
			StringTemplate st = new StringTemplate(stringBuiler.toString());
			for ( String key : page.paramMap.keySet() ) {
				//System.err.println(key);
				st.setAttribute(key, page.paramMap.get(key) );
			}
			outString = st.toString();
			outStream = new ByteArrayInputStream( outString.getBytes() );
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
	}

	@Override
	public String toString() {
		return outString;
	}


	@Override
	public int read() throws IOException {
		return outStream.read();
	}


	public static void main(String []args) throws FileNotFoundException {
		InputStream is = new FileInputStream( new File(args[0]) );
		JSRunner jsRunner = new JSRunner(null);
		//jsRunner.addInterface("note", new NoteInterface("test_db") );
		PageParser page = new PageParser(is, args[0], jsRunner );

		System.out.print( page );
	}


}
