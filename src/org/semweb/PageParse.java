package org.semweb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import javax.xml.parsers.*;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.DefaultHandler;


public class PageParse {
	
	public class PageHandler extends DefaultHandler {
		Pattern codeRE = Pattern.compile("\\&\\{(.*?)\\}\\&", Pattern.DOTALL );
		
		StringBuilder curBuffer = null;		
		
		@Override
		public void processingInstruction(String target, String data)
				throws SAXException {
			if ( target.compareTo("js") == 0 )
				XMLOutput( evalCode(data, true) );
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
					XMLOutput( " " + name.substring(5) + "='" + evalCode(value, false)  + "'" );

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
			curBuffer = new StringBuilder();
			XMLOutput(curBuffer.toString());
			XMLOutput("</" + qName + ">");
		}
		
		@Override
		public void characters(char[] ch, int start, int length)
				throws SAXException {
			curBuffer.append(ch, start, length );
		}
		
	}
	
	public String evalCode(String code, Boolean outPrint) {
		if ( code.length() > 0 )
			return codeRunner.eval( code, curPageName, outPrint );	
		return "";
	}
	
	private void XMLOutput( String str ) {
		outSB.append( str );
	}

	public String toString() {
		return outSB.toString();		
	};
	
	StringBuilder outSB;
	JSRunner codeRunner;
	String curPageName;
	
	public PageParse(InputStream is, String pageName, JSRunner codeRunner ) {		
		this.codeRunner = codeRunner;	
		curPageName = pageName;
		outSB = new StringBuilder();
		try {
			SAXParserFactory sax = SAXParserFactory.newInstance();
			SAXParser parser;
			parser = sax.newSAXParser();
			DefaultHandler ph= new PageHandler();
			parser.parse(is, ph );
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
	
	
	public static void main(String []args) throws FileNotFoundException {
		InputStream is = new FileInputStream( new File(args[0]) );
		JSRunner jsRunner = new JSRunner();
		jsRunner.addInterface("note", new NoteInterface("test_db") );
		PageParse page = new PageParse(is, args[0], jsRunner );
		System.out.print( page );		
	}
	
}
