package org.semweb.template;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Map;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.resource.loader.StringResourceLoader;

public class VelocityTemplate implements TemplateInterface {
	VelocityEngine ve;
	@Override
	public String replaceCode(String id) {
		return "$" + id;
	}

	@Override
	public void init() {
		ve = new VelocityEngine();		
		//ve.addProperty("resource.loader", "string");
		//ve.addProperty( VelocityEngine.RESOURCE_LOADER, StringResourceLoader.REPOSITORY_CLASS );
		ve.init();
	}

	@Override
	public InputStream render(String text, Map<String, String> inMap) {
		VelocityContext context = new VelocityContext();

		for ( String key : inMap.keySet() ) {
			context.put(key, inMap.get(key) );
		}
		ByteArrayOutputStream bis = new ByteArrayOutputStream();
		OutputStreamWriter outWriter = new OutputStreamWriter(bis);
		//System.out.println("Render:" + text );
		ve.evaluate(context, outWriter, "LOG", text);	
		try {
			outWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ByteArrayInputStream bos = new ByteArrayInputStream( bis.toByteArray() );
		return bos;
	}

}
