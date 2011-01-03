package org.semweb.app;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class SemWebPage extends InputStream {

	ByteArrayInputStream outStream = null;
	String outString = null;
	static public class CodeFragment {
		/*
		public CodeFragment(String source, String lang) {
			this.source = source;
			this.lang = lang;
		}
		*/
		public String source;
		public String lang;
	}
	
	String text;
	Map<String,CodeFragment> codeMap;
	
	public SemWebPage(String text, Map<String,CodeFragment> codeMap ) {
		this.text = text;
		this.codeMap = codeMap;
	}
	
	public InputStream render(Map<String,String> content) {
		if ( content != null) {
			for ( String name : content.keySet() ) {
				//st.setAttribute(name, content.get(name));
			}
		}
		//outString = st.toString();
		return new ByteArrayInputStream( text.getBytes() );
	}

	public void render() {
		render(null);		
	}


	@Override
	public int read() throws IOException {
		if ( outStream == null )
			render();
		return outStream.read();
	}
}
