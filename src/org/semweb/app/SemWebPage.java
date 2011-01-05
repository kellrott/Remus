package org.semweb.app;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.semweb.scripting.ScriptingFunction;
import org.semweb.scripting.ScriptingInterface;
import org.semweb.template.TemplateInterface;

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
		Map<String,String> inMap = new HashMap<String,String>();
		if ( paramMap != null) {
			for ( String name : paramMap.keySet() ) {
				inMap.put( name, paramMap.get(name) );
			}
		}
		
		for ( String id : codeMap.keySet() ) {
			SemWebApplet applet = codeMap.get(id);
			
			if ( applet.code.type == CodeFragment.MAPPER ) {
				inMap.put(id, "");
			}			
			if ( applet.code.type == CodeFragment.WRITER ) {
				ByteArrayOutputStream inputOut = new ByteArrayOutputStream();
				ScriptingInterface inputLang = parent.scriptMan.getLang( applet.input.inputLang );
				inputLang.setStdout( inputOut );
				inputLang.eval( applet.input.inputSource, path + ":" + id + ":input" );
				//return new ByteArrayInputStream( bout.toByteArray() );				
				ScriptingFunction func = inputLang.compileFunction( applet.code.source, path + ":" + id + ":writer");
				String out = func.call( null );
				inMap.put(id, out);
			}
		}
		TemplateInterface ti = parent.templateMan.getTemplateInterface();
		return ti.render( text, inMap );
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
