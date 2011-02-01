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

import org.mpstore.JsonSerializer;
import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.mpstore.Serializer;
import org.remus.applet.RemusApplet;

public class MasterServlet extends HttpServlet {
	RemusApp app;
	Serializer serializer;
	String workDir;
	String srcDir;
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		try {
			String mpStore = config.getInitParameter(RemusApp.configStore);
			workDir = config.getInitParameter(RemusApp.configWork);
			srcDir = config.getInitParameter(RemusApp.configSource);
			serializer = new JsonSerializer();
			Class<?> mpClass = Class.forName(mpStore);			
			MPStore store = (MPStore) mpClass.newInstance();
			store.init(serializer, workDir);			
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
				Map outMap = applet.getInfo();
				out.print( serializer.dumps(outMap) );
			} else {
				PrintWriter out = resp.getWriter();
				if ( reqInfo.api.compareTo("data") == 0 ) {
					MPStore ds = app.getDataStore();
					Map pm = req.getParameterMap();
					if ( pm.containsKey("key") && pm.containsKey("instance") ) {
						String instStr = ((String[])pm.get("instance"))[0];
						String keyStr = ((String[])pm.get("key"))[0];		
						Object keyObj = serializer.loads(keyStr);
						if ( ds.containsKey( reqInfo.file, instStr, keyObj ) ) {
							for ( Object value : ds.get( reqInfo.file, instStr, keyObj ) ) {
								out.println( serializer.dumps( value ) );
							}
						} else {
							resp.sendError( HttpServletResponse.SC_NOT_FOUND );
						}
					} else if ( pm.containsKey("instance") && pm.containsKey("jobID") && pm.containsKey("emitID" ) ) {
						String instStr = ((String[])pm.get("instance"))[0];
						String jobIDStr= ((String[])pm.get("jobID"))[0];
						String emitIDStr= ((String[])pm.get("emitID"))[0];
						
						KeyValuePair kp = ds.get( reqInfo.file, 
								instStr, 
								Long.parseLong( jobIDStr ), 
								Long.parseLong( emitIDStr ) );
						Map outmap = new HashMap();
						outmap.put( kp.getKey(), kp.getValue() );
						out.println( serializer.dumps(outmap) );
					} else if (  pm.containsKey("instance") ) {
						String instStr = ((String[])pm.get("instance"))[0];
						for ( KeyValuePair kp : ds.listKeyPairs( reqInfo.file , instStr ) ) {
							Map outMap = new HashMap();
							outMap.put(kp.getKey(), kp.getValue() );
							out.println( serializer.dumps( outMap ) );
						}						
					}
				} else if ( reqInfo.api.compareTo("keys") == 0 && req.getParameterMap().containsKey("instance") ) {
					MPStore ds = app.getDataStore();
					for ( Object key : ds.listKeys( reqInfo.file, req.getParameter("instance") ) ) {
						out.println( serializer.dumps(key) );
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
			} else if ( reqInfo.api.compareTo("work") == 0 ) {
				PrintWriter out = resp.getWriter();
				int count = 10;
				if ( req.getParameter("max") != null ) {
					count = Integer.parseInt(req.getParameter("max"));
				}
				Map<String,Map<?,?>> outMap = new HashMap<String,Map<?,?>>();
				for ( WorkDescription work : app.codeManager.getWorkQueue( count ) ) {
					if ( !outMap.containsKey(work.getInstance().toString()) )
						outMap.put( work.getInstance().toString(), new HashMap() );
					Map iMap = outMap.get(work.getInstance().toString());
					if ( !iMap.containsKey( work.getApplet().getPath() ) )
						iMap.put( work.getApplet().getPath(), new ArrayList() );
					List aList = (List)iMap.get( work.getApplet().getPath() );
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
		System.out.println( reqInfo.path );
		System.out.println( reqInfo.api );
		if ( reqInfo.path.compareTo("/") == 0 ) {
			if ( reqInfo.api.compareTo("work") == 0 ) {
				ServletInputStream is = req.getInputStream();
				ByteArrayOutputStream buff = new ByteArrayOutputStream();
				byte [] read = new byte[1024];
				int len;
				while ( (len=is.read(read)) > 0 ) {
					buff.write(read, 0, len);
				}
				Map m = (Map)serializer.loads( buff.toString() );
				for ( Object key : m.keySet() ) {
					String instStr = (String)key;
					RemusInstance inst=new RemusInstance(instStr);
					Map applets = (Map)m.get(key);
					for ( Object key2 : applets.keySet() ) {
						String appletPath = (String)key2;
						RemusApplet applet = app.codeManager.get( appletPath );
						List jobs = (List)applets.get(appletPath);
						for ( Object val : jobs ) {
							Long jobID = (Long)val;
							applet.finishWork(inst,jobID.intValue() );
						}
					}
				}
				resp.getWriter().print("\"OK\"");
			} else if ( reqInfo.api.compareTo("restart") == 0 ) {
				app = new RemusApp(new File(srcDir), app.workStore );
			}
		} else if ( app.codeManager.containsKey( reqInfo.path ) ) {
			if ( reqInfo.api != null ) {
				if ( reqInfo.api.compareTo("work") == 0 ) {
					if ( req.getParameterMap().containsKey("key") 
							&& req.getParameterMap().containsKey("instance") 
							&& req.getParameterMap().containsKey("id") 
							&& req.getParameterMap().containsKey("order")) {
						/*
						ServletInputStream is = req.getInputStream();
						ByteArrayOutputStream buff = new ByteArrayOutputStream();
						byte [] read = new byte[1024];
						int len;
						while ( (len=is.read(read)) > 0 ) {
							buff.write(read, 0, len);
						}
						 */
						app.workStore.add(reqInfo.file, 
								req.getParameter("instance"), 
								Long.parseLong(req.getParameter("id")), 
								Long.parseLong(req.getParameter("order")), 
								serializer.loads( req.getParameter("key") ), 
								serializer.loads( req.getParameter("value") ) );
						resp.getWriter().print("\"OK\"");
					}
				} 
			}
		}

	}
}

