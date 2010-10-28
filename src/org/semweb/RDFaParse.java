package org.semweb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.ext.LexicalHandler;
import org.xml.sax.helpers.DefaultHandler;

public class RDFaParse {

	class CodeHandler implements LexicalHandler {
		@Override
		public void comment(char[] ch, int start, int length)
		throws SAXException {
			outText.append(ch,start,length);			
		}

		@Override
		public void endCDATA() throws SAXException {
			outText.append("]]>");
		}

		@Override
		public void endDTD() throws SAXException {
			// TODO Auto-generated method stub

		}

		@Override
		public void endEntity(String arg0) throws SAXException {
			// TODO Auto-generated method stub

		}

		@Override
		public void startCDATA() throws SAXException {
			outText.append("<![CDATA[");
		}

		@Override
		public void startDTD(String arg0, String arg1, String arg2)
		throws SAXException {
			// TODO Auto-generated method stub

		}

		@Override
		public void startEntity(String arg0) throws SAXException {
			// TODO Auto-generated method stub

		}

	}

	class PageHandler extends DefaultHandler {

		List<String> aboutStack;
		List<Boolean> activeStack;
		StringBuilder propertyBuffer;
		@Override
		public void startDocument() throws SAXException {
			aboutStack = new LinkedList<String>();
			aboutStack.add(null);
			activeStack = new LinkedList<Boolean>();
			activeStack.add(true);
			outText = new StringBuilder();
		}

		@Override
		public void startElement(String uri, String localName, String qName,
				Attributes attributes) throws SAXException {

			if ( attributes.getValue("about") != null ) {
				aboutStack.add( attributes.getValue("about") );
			} else {
				aboutStack.add( aboutStack.get( aboutStack.size()-1 ) );
			}
			
			if ( attributes.getValue( "rel" ) != null 
					&& attributes.getValue("href") != null 
					&&  attributes.getValue( "rel" ).compareTo("internal") != 0
					&& attributes.getValue("rel").compareTo("external nofollow") != 0 ) {
				outText.append("<?rdf ");
				outText.append( aboutStack.get(aboutStack.size()-1 ) );
				outText.append( " " );
				outText.append( attributes.getValue("rel") );
				outText.append( " " );
				outText.append( attributes.getValue("href") );
				outText.append(" ?>");
				activeStack.add(false);
			} else if ( attributes.getValue( "property" ) != null ) {
				activeStack.add(false);
			} else {
				outText.append("<" + qName );
				for ( int i = 0; i < attributes.getLength(); i++ ) {
					outText.append( " " + attributes.getQName(i) + "=\"" + escapeXML(attributes.getValue(i)) + "\"" );
				}
				outText.append(">");
				activeStack.add(true);
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName)
		throws SAXException {
			if ( activeStack.get( activeStack.size()-1) )
				outText.append("</" + qName + ">" );
			activeStack.remove( activeStack.size() - 1 );
			aboutStack.remove( aboutStack.size() -1 );
		}


		@Override
		public void characters(char[] ch, int start, int length)
		throws SAXException {
			outText.append(ch, start, length);
		}
		
		
		String escapeXML(String in) {
			return in.replaceAll("&", "&amp;").replaceAll("<","&lt;" ).replace(">", "&gt;" );
		}
	}
	StringBuilder outText;
	
	InputStream ParseRDFa(InputStream is) {
		try {
			outText = new StringBuilder();
			SAXParserFactory sax = SAXParserFactory.newInstance();
			SAXParser parser;
			parser = sax.newSAXParser();
			DefaultHandler ph= new PageHandler();
			CodeHandler ch = new CodeHandler();
			parser.setProperty("http://xml.org/sax/properties/lexical-handler", ch);
			parser.parse(is, ph );
			ph.toString();
			ByteArrayInputStream out = new ByteArrayInputStream( outText.toString().getBytes() );			
			return out;
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
	
	
	public class pRDFHandler extends DefaultHandler {		
		@Override
		public void processingInstruction(String target, String data)
				throws SAXException {
			if ( target.compareTo("rdf") == 0 )
				System.out.println( data );
		}
	}	
	
	void pRDFScan(InputStream is) {
		try {
			SAXParserFactory sax = SAXParserFactory.newInstance();
			SAXParser parser;
			parser = sax.newSAXParser();
			DefaultHandler ph= new pRDFHandler();
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
		FileInputStream is = new FileInputStream(new File(args[0]));
		RDFaParse parse = new RDFaParse();	
				
		parse.pRDFScan( parse.ParseRDFa(is) );
		//System.out.println( parse.ParseRDFa( is ) );
	}


}
