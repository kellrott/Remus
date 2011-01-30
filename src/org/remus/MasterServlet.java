package org.remus;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mpstore.MPStore;
import org.remus.applet.RemusApplet;
import org.remus.data.JsonSerializer;
import org.remus.data.Serializer;

public class MasterServlet extends HttpServlet {
	RemusApp app;
	Serializer serializer;
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		try {
			String mpStore = config.getInitParameter(RemusApp.configStore);
			String workDir = config.getInitParameter(RemusApp.configWork);
			String srcDir = config.getInitParameter(RemusApp.configSource);
			serializer = new JsonSerializer();
			Class<?> mpClass = Class.forName(mpStore);			
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
				//applet request, no api mode
				PrintWriter out = resp.getWriter();
				RemusApplet applet = app.codeManager.get( reqInfo.path );
				Map outMap = applet.getDescMap();
				out.print( serializer.dumps(outMap) );
			} else {
				PrintWriter out = resp.getWriter();
				if ( reqInfo.api.compareTo("data") == 0 && req.getParameterMap().containsKey("key") && req.getParameterMap().containsKey("instance") ) {
					MPStore ds = app.getDataStore();
					if ( ds.containsKey( reqInfo.file, (String)req.getParameterMap().get("instance"), (String)req.getParameterMap().get("key")  ) ) {
						for ( Serializable value : ds.get( reqInfo.file, (String)req.getParameterMap().get("instance"), (String)req.getParameterMap().get("key") ) ) {
							out.println( value );
						}
					} else {
						resp.sendError( HttpServletResponse.SC_NOT_FOUND );
					}
				} else if ( reqInfo.api.compareTo("keys") == 0 && req.getParameterMap().containsKey("instance") ) {
					MPStore ds = app.getDataStore();
					for ( Comparable key : ds.listKeys( reqInfo.file, req.getParameter("instance") ) ) {
						out.println( key );
					}
				} else if ( reqInfo.api.compareTo("code") == 0  ) {
					out.write( app.codeManager.get(reqInfo.path).getSource() );
				} else if ( reqInfo.api.compareTo("work") == 0 ) {
					RemusApplet applet = app.codeManager.get(reqInfo.path);
					String instStr = req.getParameter("instance");
					String idStr = req.getParameter("id");
					if ( instStr != null && idStr != null ) {
						if ( applet.getPipeline() != null ) {
							RemusInstance inst = new RemusInstance( instStr );
							int jobID = Integer.parseInt(idStr);
							WorkDescription w = applet.getWork(inst, jobID);
							if ( w != null )
								out.println( serializer.dumps( w.getDesc() ) );
						}
					}
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
				int count = 10;
				if ( req.getParameter("max") != null ) {
					count = Integer.parseInt(req.getParameter("max"));
				}
				Map<String,Map<?,?>> outMap = new HashMap<String,Map<?,?>>();
				for ( RemusWork work : app.codeManager.getWorkQueue( count ) ) {
					if ( !outMap.containsKey(work.instance.toString()) )
						outMap.put( work.instance.toString(), new HashMap() );
					Map iMap = outMap.get(work.instance.toString());
					if ( !iMap.containsKey( work.applet.getPath() ) )
						iMap.put( work.applet.getPath(), new ArrayList() );
					List aList = (List)iMap.get( work.applet.getPath() );
					aList.add(work.jobID);
				}
				out.print( serializer.dumps(outMap) );
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

		if ( reqInfo.path.compareTo("/") == 0 ) {
			if ( reqInfo.api.compareTo("work") == 0 ) { 
				String instStr = req.getParameter("instance");
				String idStr = req.getParameter("id");
				RemusApplet applet = app.codeManager.get( reqInfo.path );
				applet.finishWork( new RemusInstance(instStr), Integer.parseInt(idStr) );
			}
		} else if ( app.codeManager.containsKey( reqInfo.path ) ) {
			if ( reqInfo.api != null ) {
				if ( reqInfo.api.compareTo("work") == 0 ) {
					if ( req.getParameterMap().containsKey("key") && req.getParameterMap().containsKey("instance") ) {
						/*
						ServletInputStream is = req.getInputStream();
						ByteArrayOutputStream buff = new ByteArrayOutputStream();
						byte [] read = new byte[1024];
						int len;
						while ( (len=is.read(read)) > 0 ) {
							buff.write(read, 0, len);
						}
						*/
						app.workStore.add(reqInfo.file, req.getParameter("instance"), req.getParameter("key"), req.getParameter("value") );
						resp.getWriter().println("Done");
						return;
					}
				}
			}

		}

	}
}

