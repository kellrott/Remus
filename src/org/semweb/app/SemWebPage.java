package org.semweb.app;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.semweb.scripting.ScriptingInterface;

public class SemWebPage extends InputStream {

	ByteArrayInputStream outStream = null;
	String outString = null;
	
	
	String text;
	Map<String,SemWebApplet> codeMap;
	PageManager parent;
	String path;
	public SemWebPage(PageManager parent, String path, String text, Map<String,SemWebApplet> codeMap ) {
		this.parent = parent;
		this.text = text;
		this.codeMap = codeMap;
		this.path = path;
	}
	
	public InputStream render(Map<String,String> paramMap) {
		if ( paramMap != null) {
			for ( String name : paramMap.keySet() ) {
				//st.setAttribute(name, content.get(name));
			}
		}
		
		for ( String id : codeMap.keySet() ) {
			SemWebApplet applet = codeMap.get(id);
			if ( applet.code.type == CodeFragment.WRITER ) {
				ByteArrayOutputStream bout = new ByteArrayOutputStream();
				ScriptingInterface inputLang = parent.scriptMan.getLang( applet.input.inputLang );
				inputLang.setStdout( bout );
				inputLang.eval( applet.input.inputSource, path + ":" + id );
				//return new ByteArrayInputStream( bout.toByteArray() );
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
