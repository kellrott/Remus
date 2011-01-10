package org.semweb.app;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.semweb.pluginterface.InterfaceBase;
import org.semweb.pluginterface.WriterInterface;
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
				InterfaceBase inputLang = parent.plugMan.getPlugin( applet.code.lang );
				if ( inputLang instanceof WriterInterface ) {
					WriterInterface writer = (WriterInterface)inputLang;
					writer.prepWriter( applet.code.source );

					
					Object obj;
					if ( applet.input != null )
						obj = writer.write(  applet.input.getContent() );
					else
						obj = writer.write(  null );
					if ( obj != null )
						inMap.put(id, obj.toString() );
					else 
						inMap.put(id, null);
				}

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
