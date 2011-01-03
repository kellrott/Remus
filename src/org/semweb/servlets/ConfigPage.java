package org.semweb.servlets;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.semweb.config.SemwebConfig;

import java.net.URL;


public class ConfigPage extends HttpServlet {

	private static final long serialVersionUID = 2717820796120041059L;
	public String configPath;
	SemwebConfig config;
	@Override
	public void init() throws ServletException {
		configPath = getServletContext().getRealPath( "WEB-INF/SemWebConfig.xml" );
		config = new SemwebConfig( new File( configPath ) );
	}

	public void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException{
		/*  first, get and initialize an engine  */
		URL url = this.getClass().getResource("Config.vt");

		File file = new File(url.getFile()).getParentFile();

		VelocityEngine ve = new VelocityEngine();
		ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "file");
		ve.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_PATH, file.getAbsolutePath());
		ve.setProperty(RuntimeConstants.FILE_RESOURCE_LOADER_CACHE, "true");

		ve.init();

		VelocityContext context = new VelocityContext();
		context.put("apps",  config.appList );
		context.put("config", config);

		Template t = ve.getTemplate( "Config.vt" );
		
		t.merge( context, res.getWriter() );
	}
}
