package org.remus;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

	Pattern appletSub = Pattern.compile("(\\:\\w+)\\.(\\w+)$");

	class RequestInfo {
		public String path;
		public String appletPath;
		public String appletSubName;
		public String api;
		File file, srcFile;

		public RequestInfo( String pathinfo ) {
			String [] tmp = pathinfo.split("@");
			path = tmp[0];
			api = null;
			appletSubName = null;
			if ( tmp.length > 1 ) {
				api = tmp[1];
			}
			Matcher m = appletSub.matcher( tmp[0] );
			if ( m.find() ) {
				String appletName = m.group(1);
				appletSubName = m.group(2);
				appletPath = m.replaceAll(appletName);
			} else {
				appletPath = tmp[0];
			}
			appletPath = (new File(appletPath)).getAbsolutePath();
			file = new File( path );	
			path = file.getAbsolutePath();
			srcFile = new File(app.srcbase, path );		
		}

		public boolean isSubName() {
			if ( appletSubName == null )
				return false;
			return true;
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {		
		RequestInfo reqInfo = new RequestInfo( req.getPathInfo() );
		System.out.println( reqInfo.path );
		if ( app.codeManager.containsKey( reqInfo.appletPath ) ) {
			if ( reqInfo.api == null ) {
				PrintWriter out = resp.getWriter();				
				resp.setContentType( "text/html" );
				out.println( "<p><a href='" + reqInfo.path + "@code'>CODE</a></p>" );

				RemusApplet applet = app.codeManager.get(reqInfo.appletPath);
				String instStr = req.getParameter("instance");
				RemusInstance curInst = null;
				if ( instStr != null )
					curInst = new RemusInstance(instStr);


				out.println("INPUTS<ul>");
				for ( InputReference iRef : applet.getInputs() ) {
					if ( instStr != null )
						out.println( "<li><a href='" + iRef.getPath() + "?instance=" + instStr + "'>" + iRef.getPath() + "</a></li>" );
					else
						out.println( "<li><a href='" + iRef.getPath() + "'>" + iRef.getPath() + "</a></li>" );
				}
				out.println("</ul>");

				out.println("OUTPUT<ul>");
				if ( instStr != null )
					out.println( "<li><a href='" + applet.getPath() + "?instance=" + instStr + "'>" + applet.getPath() + "</a></li>" );
				else
					out.println( "<li><a href='" + applet.getPath() + "'>" + applet.getPath() + "</a></li>" );

				for ( String output : applet.getOutputs() ) {
					if ( instStr != null )
						out.println( "<li><a href='" + applet.getPath() + "." + output + "?instance=" + instStr + "'>" + applet.getPath() + "." + output + "</a></li>" );
					else
						out.println( "<li><a href='" + applet.getPath() + "." + output + "'>" + applet.getPath() + "." + output + "</a></li>" );
				}
				out.println( "</ul>" );

				if ( instStr != null ) {
					if ( applet.isComplete(curInst ) ) {
						out.println("<p>Work Complete</p>");
					} else { 
						if ( applet.isReady(curInst) ) {
							out.println("<p>Work Ready</p>" );
						} else {
							out.println("<p>Work Not Ready</p>" );							
						}
					}
					out.println("<ul>");
					out.println( "<li><a href='" + reqInfo.path + "@keys?instance="   + instStr + "'>KEYS</a></li>" );
					out.println( "<li><a href='" + reqInfo.path + "@data?instance="   + instStr + "'>DATA</a></li>" );					
					out.println( "<li><a href='" + reqInfo.path + "@reduce?instance=" + instStr + "'>REDUCE</a></li>" );					
					out.println("</ul>");
				} else {
					for ( RemusInstance inst : applet.getInstanceList() ) {
						out.println( "<a href='" + reqInfo.path + "?instance=" + inst.toString() + "'>" + inst.toString() + "</a>" );
					}
				}
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
					out.write( app.codeManager.get(reqInfo.appletPath).getSource() );
				} else if ( reqInfo.api.compareTo("work") == 0 ) {
					RemusApplet applet = app.codeManager.get(reqInfo.appletPath);
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
				} else if ( reqInfo.api.compareTo("info") == 0 ) {
					RemusApplet applet = app.codeManager.get( reqInfo.appletPath );
					Map outMap = applet.getInfo();
					out.print( serializer.dumps(outMap) );
				} else if ( reqInfo.api.compareTo("reduce") == 0 ) {
					MPStore ds = app.getDataStore();
					String instStr = req.getParameter("instance");
					for ( Object key : ds.listKeys( reqInfo.file, instStr ) ) {
						Map outMap = new HashMap();
						List outList = new ArrayList();
						for ( Object val : ds.get(reqInfo.file, instStr, key) ) {
							outList.add(val);							
						}
						outMap.put(key, outList);
						out.println( serializer.dumps( outMap ) );
					}
				}
			}
		} else if ( reqInfo.path.compareTo("/") == 0 ) {
			if ( reqInfo.api == null ) {
				PrintWriter out = resp.getWriter();
				resp.setContentType( "text/html" );
				out.println( "Pipelines: <ul>");
				for ( int i =0; i < app.codeManager.getPipelineCount(); i++ ) {
					RemusPipeline pipeline = app.codeManager.getPipeline(i);
					if ( pipeline.dynamic )
						out.println( "<li><a href='/@pipeline?id=" + i + "'>Dynamic Pipeline " + i + "</a></li>" );
					else
						out.println( "<li><a href='/@pipeline?id=" + i + "'>Static Pipeline " + i + "</a></li>" );
					out.println("<h3>CodeList</h3><ul>");
					for ( RemusApplet applet : pipeline.getMembers() ) {
						out.println( "<li><a href='" + applet.getPath() + "'>" + applet.getPath() + "</a></li>" );
					}
					out.println("</ul>");


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
		if ( reqInfo.path.compareTo("/") == 0 ) {
			if ( reqInfo.api.compareTo("restart") == 0 ) {
				app = new RemusApp(new File(srcDir), app.workStore );
			}
		} else if ( app.codeManager.containsKey( reqInfo.appletPath ) ) {
			if ( reqInfo.api != null ) {
				if ( reqInfo.api.compareTo("work") == 0 ) {
					BufferedReader br = req.getReader();
					String curline = null;
					while ((curline=br.readLine())!= null ) {
						Map m = (Map)serializer.loads( curline );
						for ( Object key : m.keySet() ) {
							String instStr = (String)key;
							RemusInstance inst=new RemusInstance(instStr);
							List jobList = (List)m.get(key);
							RemusApplet applet = app.codeManager.get( reqInfo.appletPath );
							for ( Object key2 : jobList ) {
								Long jobID = (Long)key2;
								applet.finishWork(inst,jobID.intValue() );
							}						
						}
					}
					resp.getWriter().print("\"OK\"");
				} else if ( reqInfo.api.compareTo("data") == 0 ) {
					BufferedReader br = req.getReader();
					String curline = null;
					while ( (curline = br.readLine() ) != null ) {
						Map inObj = (Map)serializer.loads(curline);	
						app.workStore.add(reqInfo.file, 
								(String)inObj.get("instance"), 
								(Long)inObj.get("id"), 
								(Long)inObj.get("order"), 
								inObj.get("key") , 
								inObj.get("value") );
					}
					resp.getWriter().print("\"OK\"");
				}
			}
		} 
	}
}




