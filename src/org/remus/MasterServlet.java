package org.remus;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mpstore.MPStore;

public class MasterServlet extends HttpServlet {
	RemusApp app;
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);

		try {
			String mpStore = config.getInitParameter(RemusApp.configStore);
			String workDir = config.getInitParameter(RemusApp.configWork);
			String srcDir = config.getInitParameter(RemusApp.configSource);

			Class mpClass = Class.forName(mpStore);			
			MPStore store = (MPStore) mpClass.newInstance();
			store.init(workDir);			
			app = new RemusApp(new File(srcDir), store);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = -8067165004515233805L;

	class RequestInfo {
		public String path;
		public String api;
		File file, srcFile;

		public RequestInfo( String pathinfo ) {
			String [] tmp = pathinfo.split("@");
			path = tmp[0];
			api = null;
			if ( tmp.length > 1 ) {
				api = tmp[1];
			}
			file = new File( path );	
			path = file.getAbsolutePath();
			srcFile = new File(app.srcbase, path );		
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {		
		RequestInfo reqInfo = new RequestInfo( req.getPathInfo() );

		if ( app.codeManager.containsKey( reqInfo.path ) ) {
			if ( reqInfo.api == null ) {
				PrintWriter out = resp.getWriter();

				RemusApplet applet = app.codeManager.get( reqInfo.path );
				resp.setContentType( "text/html" );
				out.println( "APPLET: " + applet.getPath()  );
				out.println( "<br/>CODE: <a href='" + applet.getPath() + "@code'> CODE </a>" );
				out.println("<ul>");
				for ( InputReference input : applet.getInputs() ) {
					out.println( "<li>INPUT: <a href='" + input.finalURL + "'>" + input.finalURL + "</a></li>");
				}
				out.println("</ul>");

			} else {
				PrintWriter out = resp.getWriter();

				if ( req.getParameterMap().containsKey("key") ) {
					MPStore ds = app.getDataStore();
					if ( ds.containsKey( reqInfo.file, (String)req.getParameterMap().get("key")  ) ) {
						for ( Serializable value : ds.get( reqInfo.file, (String)req.getParameterMap().get("key") ) ) {
							out.println( value );
						}
					} else {
						resp.sendError( HttpServletResponse.SC_NOT_FOUND );
					}
				} else if ( reqInfo.api.compareTo("keys") == 0 ) {
					MPStore ds = app.getDataStore();
					for ( Comparable key : ds.listKeys( reqInfo.file ) ) {
						out.println( key );
					}
				} else if ( reqInfo.api.compareTo("code") == 0  ) {
					out.write( app.codeManager.get(reqInfo.path).getSource() );
				}
			}
		} else if ( reqInfo.path.compareTo("/") == 0 ) {
			if ( reqInfo.api == null ) {
				PrintWriter out = resp.getWriter();
				resp.setContentType( "text/html" );
				out.println( "Code list: <ul>");
				for ( String path : app.codeManager.keySet() ) {
					out.println( "<li><a href='" + path + "'>" + path + "</a>" );
				}
				out.println("</ul>");
				out.println( "Code list: <ul>");
				for ( RemusPipeline pipeline : app.codeManager.pipelines ) {
					if ( pipeline.dynamic )
						out.println( "<li> Dynamic " + pipeline + "</li>" );
					else
						out.println( "<li> Static " + pipeline + "</li>" );
					for ( RemusApplet applet : pipeline.getMembers() ) {
						out.println( "----- " + applet.getPath() + "<br/>" );
					}
				}
				out.println("</ul>");
			} else {
				PrintWriter out = resp.getWriter();
				for ( RemusWork work : app.codeManager.getWorkQueue( 10 ) ) {
					out.println( work );
				}
			}
		} else if (reqInfo.srcFile.exists() ) {
			FileInputStream fis = new FileInputStream( reqInfo.srcFile );
			ServletOutputStream os = resp.getOutputStream();
			byte [] buffer = new byte[1024];
			int len;
			while ( (len = fis.read(buffer)) >= 0 ) {
				os.write( buffer, 0, len );
			}
		} else {
			PrintWriter out = resp.getWriter();
			out.write( "Not Found:" + reqInfo.path );
		}
	}


	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		RequestInfo reqInfo = new RequestInfo( req.getPathInfo() );

		if ( app.codeManager.containsKey( reqInfo.path ) ) {
			if ( reqInfo.api != null ) {
				if ( req.getParameterMap().containsKey("key") ) {
					ServletInputStream is = req.getInputStream();
					ByteArrayOutputStream buff = new ByteArrayOutputStream();
					byte [] read = new byte[1024];
					int len;
					while ( (len=is.read(read)) > 0 ) {
						buff.write(read, 0, len);
					}
					app.workStore.add(reqInfo.file, req.getParameter("key"), buff.toString() );
					resp.getWriter().println("Done");
					return;
				}
			}
		}

	}


}
