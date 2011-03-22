package org.remus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mpstore.AttachStore;
import org.mpstore.JsonSerializer;
import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.mpstore.Serializer;
import org.remus.manage.WorkManager;
import org.remus.work.RemusApplet;

/**
 * MasterServlet: Primary servlet interface for web based Remus Server.
 * @author kellrott
 *
 */

public class MasterServlet extends HttpServlet {
	RemusApp app;
	Serializer serializer;
	//	String workDir;
	String srcDir;
	Map<String,String> configMap;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		try {
			configMap = new HashMap<String,String>();			
			Enumeration names = config.getInitParameterNames();
			while ( names.hasMoreElements() ) {
				String name = (String) names.nextElement();
				configMap.put(name, config.getInitParameter(name));
			}
			serializer = new JsonSerializer();
			app = new RemusApp(configMap);
		} catch (RemusDatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = -8067165004515233805L;


	private String getWorkerID( HttpServletRequest req ) {
		String workerID = null;
		if ( req.getCookies() != null ) {
			for ( Cookie cookie : req.getCookies() ) {
				if ( cookie.getName().compareTo( "remusWorker" ) == 0 ) {
					workerID = cookie.getValue();
				}
			}
		}
		return workerID;
	}


	private void doGet_pipeline(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {

		if ( reqInfo.getApplet() != null && app.hasApplet( reqInfo.getAppletPath() ) ) {
			PrintWriter out = resp.getWriter();
			RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
			MPStore ds = applet.getDataStore();
			Object outObj = null;
			for ( Object obj : ds.get( "/" + reqInfo.getPipeline() + "@pipeline", RemusInstance.STATIC_INSTANCE_STR, reqInfo.getApplet() ) ) {
				outObj = obj;
			}
			Map instMap = new HashMap();
			for ( RemusInstance inst : applet.getInstanceList() ) {
				instMap.put( inst.toString(), applet.getInstanceSubmit(inst ) );
			}
			((Map)outObj).put("instance", instMap );
			out.println( serializer.dumps( outObj ) );
		} else {
			if ( reqInfo.getPipeline() != null ) {
				RemusPipeline pipe = app.pipelines.get(reqInfo.getPipeline() );
				PrintWriter out = resp.getWriter();
				Map outMap = new HashMap();
				for ( RemusApplet applet : pipe.getMembers() ) {
					Map pipeMap = new HashMap();
					List instList = new ArrayList();
					for ( RemusInstance inst : applet.getInstanceList() ) {
						instList.add( inst.toString() );
					}
					pipeMap.put("instance", instList );
					outMap.put(applet.getID(), pipeMap);
				}
				out.print( serializer.dumps(outMap) );

			} else {			
				PrintWriter out = resp.getWriter();
				Map outMap = new HashMap();
				for ( RemusPipeline pipe : app.getPipelines() ) {
					List pipeMap = new ArrayList();
					for ( RemusApplet applet : pipe.getMembers() ) {
						pipeMap.add(applet.getID());
					}
					outMap.put(pipe.getID(), pipeMap);
				}
				out.print( serializer.dumps(outMap) );
			}


		}

	}


	private void doGet_work(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		PrintWriter out = resp.getWriter();
		int count = 10;
		if ( req.getParameter("max") != null ) {
			count = Integer.parseInt(req.getParameter("max"));
		}
		String workerID = getWorkerID(req);
		if ( workerID != null ) {
			Object outVal = app.getWorkManager().getWorkMap( workerID, count );
			out.print( serializer.dumps(outVal) );
		} else {
			resp.sendError( HttpServletResponse.SC_NOT_FOUND );
		}
	}

	private void doGet_data(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		PrintWriter out = resp.getWriter();
		resp.setBufferSize(2048);
		RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
		MPStore ds = applet.getDataStore();
		if ( reqInfo.getInstance() != null && reqInfo.getKey() != null ) {
			String instStr = (new RemusInstance(ds, reqInfo.getInstance())).toString();
			String keyStr = reqInfo.getKey();
			String sliceStr = req.getParameter("slice");
			if ( sliceStr == null ) {
				if ( ds.containsKey( reqInfo.getPortPath() + "@" + reqInfo.getView(), instStr, keyStr ) ) {
					for ( Object value : ds.get( reqInfo.getPortPath() + "@" + reqInfo.getView(), instStr, keyStr ) ) {
						Map oMap = new HashMap();
						oMap.put( keyStr, value);
						out.println( serializer.dumps( oMap ) );
						resp.flushBuffer();
					}
				} else {
					resp.sendError( HttpServletResponse.SC_NOT_FOUND );
				}
			} else {
				int sliceSize = Integer.parseInt(sliceStr);
				for ( String sliceKey : ds.keySlice( reqInfo.getPortPath() + "@" + reqInfo.getView(), instStr, keyStr, sliceSize) ) {
					for ( Object value : ds.get( reqInfo.getPortPath() + "@" + reqInfo.getView(), instStr, sliceKey ) ) {
						Map oMap = new HashMap();
						oMap.put( sliceKey, value);
						out.println( serializer.dumps( oMap ) );
						resp.flushBuffer();
					}
				}
			}
		} else if ( reqInfo.getInstance() != null) {
			String instStr = (new RemusInstance(ds, reqInfo.getInstance())).toString();
			for ( KeyValuePair kp : ds.listKeyPairs( reqInfo.getPortPath() + "@" + reqInfo.getView() , instStr ) ) {
				Map outMap = new HashMap();
				outMap.put(kp.getKey(), kp.getValue() );
				out.println( serializer.dumps( outMap ) );
				out.flush();
				resp.flushBuffer();
			}						
		}
	}


	private void doGet_submit(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {	
		RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
		if ( applet != null ) {		
			PrintWriter out = resp.getWriter();
			MPStore ds = applet.getDataStore();
			for ( KeyValuePair kv : ds.listKeyPairs( reqInfo.getPortPath() + "@" + reqInfo.getView(), RemusInstance.STATIC_INSTANCE_STR ) ) {
				Map outMap = new HashMap();
				Map instMap = new HashMap();
				instMap.put( kv.getValue(), applet.getInstanceSrc( new RemusInstance((String)kv.getValue()) ) );
				outMap.put( applet.getPath(), instMap );
				out.println( serializer.dumps( outMap ) );
				out.flush();
			}
		} else {
			PrintWriter out = resp.getWriter();
			for ( RemusPipeline pipe : app.pipelines.values() ) {
				for ( KeyValuePair kv : pipe.getSubmits() ) {
					Map outMap = new HashMap();		
					outMap.put( kv.getKey(), kv.getValue() );
					out.println( serializer.dumps( outMap ) );						
				}
			}
		}
	}

	private void doGet_keys(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		PrintWriter out = resp.getWriter();
		resp.setBufferSize(2048);
		RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
		MPStore ds = applet.getDataStore();
		String instStr = (new RemusInstance(ds, reqInfo.getInstance())).toString();
		for ( Object key : ds.listKeys( reqInfo.getPortPath() + "@data", instStr ) ) {
			out.println( serializer.dumps(key) );
		}
	}

	private void doGet_reduce(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		PrintWriter out = resp.getWriter();
		resp.setBufferSize(2048);
		RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
		MPStore ds = applet.getDataStore();
		for ( String key : ds.listKeys( reqInfo.getPortPath() + "@data", reqInfo.getInstance() ) ) {
			Map outMap = new HashMap();
			List outList = new ArrayList();
			for ( Object val : ds.get(reqInfo.getPortPath() + "@data", reqInfo.getInstance(), key) ) {
				outList.add(val);							
			}
			outMap.put(key, outList);
			out.println( serializer.dumps( outMap ) );
		} 
	}

	private void doGet_instance(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {		
		PrintWriter out = resp.getWriter();
		if ( reqInfo.getApplet() != null ) {
			RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
			if ( applet != null ) {
				MPStore ds = applet.getDataStore();
				List outList = new LinkedList();
				for ( KeyValuePair kv : ds.listKeyPairs( reqInfo.getAppletPath() + "@instance", RemusInstance.STATIC_INSTANCE_STR) ) {
					Map outmap = new HashMap();
					outmap.put(kv.getKey(), kv.getValue() );
					outList.add( outmap );
				}
				out.println( serializer.dumps( outList ) );
			}
		} else if ( reqInfo.getPipeline() != null ) {
			RemusPipeline pipe = app.pipelines.get(reqInfo.getPipeline());
			if ( pipe != null ) {
				Map<String,Object> oSet = new HashMap<String,Object>();
				for ( RemusApplet applet : pipe.getMembers() ) {
					for ( RemusInstance inst : applet.getInstanceList() ) {
						oSet.put( inst.toString(), applet.getInstanceSubmit( inst ) );
					}
				}
				out.println( serializer.dumps( oSet ) );
			}
		} else {
			Map<String,Object> oSet = new HashMap<String,Object>();
			for ( RemusPipeline pipe : app.getPipelines() ) {
				for ( RemusApplet applet : pipe.getMembers() ) {
					Map aMap = new HashMap();
					for ( RemusInstance inst : applet.getInstanceList() ) {
						aMap.put(inst.toString(), applet.getInstanceSubmit(inst));
					}
					oSet.put(applet.getPath(), aMap);
				}
			}
			out.println( serializer.dumps( oSet ) );
		}
	}

	private void doGet_attach(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		if ( reqInfo.getApplet() != null && app.hasApplet( reqInfo.getAppletPath() ) ) {
			if ( reqInfo.getInstance() != null && reqInfo.getKey() != null ) {
				RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
				AttachStore ds = applet.getAttachStore();
				InputStream is = ds.readAttachement( reqInfo.getAppletPath() + "@attach", reqInfo.getInstance(), reqInfo.getKey() );
				if ( is != null ) {
					ServletOutputStream os = resp.getOutputStream();
					byte [] buffer = new byte[1024];
					int len;
					while ( (len = is.read(buffer)) >= 0 ) {
						os.write( buffer, 0, len );
					}
					os.close();
				} else {
					resp.sendError( HttpServletResponse.SC_NOT_FOUND );
				}
			} else if (  reqInfo.getInstance() != null && reqInfo.getKey() == null ) {
				PrintWriter out = resp.getWriter();
				RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
				AttachStore ds = applet.getAttachStore();
				for ( String val : ds.listKeys(reqInfo.getAppletPath() + "@attach", reqInfo.getInstance()  ) )  {
					out.println( serializer.dumps( val ) );
				}
			}
		} else {
			RemusPipeline pipeline = app.pipelines.get( reqInfo.getPipeline() );
			if ( reqInfo.getKey() != null ) { 
				InputStream is = pipeline.attachStore.readAttachement("/" + pipeline.getID() +"@attach" , RemusInstance.STATIC_INSTANCE_STR, reqInfo.getKey() );
				if ( is != null ) {
					ServletOutputStream os = resp.getOutputStream();
					byte [] buffer = new byte[1024];
					int len;
					while ( (len = is.read(buffer)) >= 0 ) {
						os.write( buffer, 0, len );
					}
					os.close();
				}
			} else {
				List<String> outList = pipeline.attachStore.listKeys( "/" + pipeline.getID() +"@attach" , RemusInstance.STATIC_INSTANCE_STR );
				PrintWriter out = resp.getWriter();
				out.print( serializer.dumps( outList ));
			}
		}
	}

	private void doGet_status(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {

		if ( reqInfo.getApplet() != null ) {
			RemusApplet applet = app.getApplet(reqInfo.getAppletPath() );
			if ( applet != null ) {
				PrintWriter out = resp.getWriter();
				if ( reqInfo.getInstance() != null ) {
					out.print( serializer.dumps( applet.getStatus( new RemusInstance(reqInfo.getInstance()) ) ) );
				} else { 
					for ( RemusInstance inst : applet.getInstanceList() ) {
						out.print( serializer.dumps( applet.getStatus( inst ) ) ) ;

					}
				}

			}

		} else {		
			String workerID = getWorkerID(req);
			if ( workerID != null ) {
				RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
				app.getWorkManager().touchWorkerStatus( workerID );
			}

			PrintWriter out = resp.getWriter();
			Map outMap = new HashMap();				
			Map workerMap = new HashMap();
			for ( String wID : app.getWorkManager().getWorkers()) {
				//TODO: put in more methods to access work manager statistics
				Map curMap = new HashMap();
				curMap.put("activeCount", app.getWorkManager().getWorkerActiveCount(wID) );
				Date lastDate = app.getWorkManager().getLastAccess(wID);
				if ( lastDate != null )
					curMap.put("lastContact", System.currentTimeMillis() - lastDate.getTime()  );
				workerMap.put(wID, curMap );	
			}
			Map<RemusApplet, Integer> assignMap = app.getWorkManager().getAssignRateMap();
			Map aMap = new HashMap();
			for ( RemusApplet applet : assignMap.keySet() ) {
				aMap.put(applet.getPath(), assignMap.get(applet) );
			}
			outMap.put( "assignRate", aMap );
			outMap.put( "workers", workerMap );
			outMap.put( "workBufferSize", app.getWorkManager().getWorkBufferSize() );
			//outMap.put("finishRate", workManage.getFinishRate() );
			out.print( serializer.dumps(outMap) );
		}
	}

	private void doGet_config(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		PrintWriter out = resp.getWriter();
		out.print( serializer.dumps(app.params) );
	}
	private void doGet_template(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		if ( reqInfo.getApplet() != null ) {
			if ( app.hasApplet( reqInfo.getAppletPath() ) ) {
				RemusPipeline pipe = app.getPipeline( reqInfo.getPipeline() );
				PrintWriter out = resp.getWriter();				
				resp.setContentType( "text/html" );
				out.println( "<p><a href='../'>MAIN</a></p>" );

				out.println( "<p><a href='" + reqInfo.getAppletPath() + "@code'>CODE</a></p>" );

				RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
				String instStr = reqInfo.getInstance();
				RemusInstance curInst = null;
				if ( instStr != null ) {
					curInst = new RemusInstance(applet.getDataStore(), instStr);
					instStr = curInst.toString();
				}
				for ( RemusPath iref : applet.getInputs() )	{
					if ( iref.getInputType() == RemusPath.DynamicInput ) {
						out.println("SUBMISSION<ul>");
						for ( KeyValuePair kv : pipe.getSubmits() ) {
							out.println( "<li>" + kv.getKey() + "<blockquote>" + kv.getValue() + "</blockquote></li>" );
						}
						out.println("</ul>");
					}
				}

				out.println("Instance<ul>");
				for ( RemusInstance inst : applet.getInstanceList() ) {					
					out.println( "<li>" + inst.toString() + " " + applet.getInstanceSubmit(inst) + "</li>" );
				}
				out.println("</ul>");


				out.println("INPUTS<ul>");
				for ( RemusPath iRef : applet.getInputs() ) {
					if ( instStr != null )
						out.println( "<li><a href='" + iRef.getPortPath() + "@/" + instStr + "'>" + iRef.getPortPath() + "</a></li>" );
					else
						out.println( "<li><a href='" + iRef.getPortPath() + "'>" + iRef.getPortPath() + "</a></li>" );
				}
				out.println("</ul>");

				out.println("OUTPUT<ul>");
				if ( instStr != null )
					out.println( "<li><a href='" + applet.getPath() + "@/" + instStr + "'>" + applet.getPath() + "</a></li>" );
				else
					out.println( "<li><a href='" + applet.getPath() + "'>" + applet.getPath() + "</a></li>" );

				for ( String output : applet.getOutputs() ) {
					if ( instStr != null )
						out.println( "<li><a href='" + applet.getPath() + "." + output + "@/" + instStr + "'>" + applet.getPath() + "." + output + "</a></li>" );
					else
						out.println( "<li><a href='" + applet.getPath() + "." + output + "'>" + applet.getPath() + "." + output + "</a></li>" );
				}
				out.println( "</ul>" );

				if ( instStr != null ) {
					if ( applet.isComplete(curInst ) ) {
						out.println("<p>Work Complete</p>");
					} else { 
						if ( applet.isInError(curInst) ) {
							out.println("<p>Error</p>" );
						} else {
							if ( applet.isReady(curInst) ) {
								if ( applet.isComplete(curInst) )
									out.println("<p>Work Ready</p>" );
								else
									out.println("<p>Work Pending</p>" );
							} else {
								out.println("<p>Work Not Ready</p>" );							
							}
						}
					}
					if ( applet.isReady(curInst) || applet.getType() == RemusApplet.STORE ) {
						out.println("<ul>");
						out.println( "<li><a href='" + reqInfo.getPortPath() + "@keys/"   + instStr + "'>KEYS</a></li>" );
						out.println( "<li><a href='" + reqInfo.getPortPath() + "@data/"   + instStr + "'>DATA</a></li>" );					
						out.println( "<li><a href='" + reqInfo.getPortPath() + "@reduce/" + instStr + "'>REDUCE</a></li>" );					
						out.println("</ul>");
					}
					MPStore ds = applet.getDataStore();
					boolean first = true;

					if ( applet.getType() == RemusApplet.PIPE ) {		
						/*
					//TODO: setup attachment listing system
					first = true;
					for ( String key : ds.listKeys( applet.getPath() + "@attach", curInst.toString() ) ) {
						if ( first ) {
							out.println( "<p>Attachments</p>" );
							first = false;
						}
						out.println( "<li><a href='" + reqInfo.getAppletPath() + "@attach/"   + instStr + "/" + key + "'>" + key + "</a></li>" );
					}						
						 */
					}

					first = true;
					for ( KeyValuePair kv : ds.listKeyPairs(applet.getPath() + "@error", curInst.toString() ) ) {
						if ( first ) {
							out.println( "<p>ERRORS</p>" );
							first = false;
						}
						out.println( "<li>" + kv.getValue() + "</li>" );
					}

				} else {
					for ( RemusInstance inst : applet.getInstanceList() ) {
						out.println( "<li><a href='" + reqInfo.getPortPath() + "@/" + inst.toString() + "'>" + inst.toString() + "</a></li>" );
					}
				}
			}
		} else if (reqInfo.getPipeline() != null && reqInfo.getKey() != null ) {
			RemusPipeline pipe = app.pipelines.get(reqInfo.getPipeline());
			InputStream is = pipe.getAttachStore().readAttachement("/" + reqInfo.getPipeline() + "@attach", RemusInstance.STATIC_INSTANCE_STR, reqInfo.getKey() );
			ServletOutputStream os = resp.getOutputStream();
			byte [] buffer = new byte[1024];
			int len;
			while ( (len = is.read(buffer)) >= 0 ) {
				os.write( buffer, 0, len );
			}
			os.close();
			is.close();
		} else {

			PrintWriter out = resp.getWriter();
			resp.setContentType( "text/html" );
			out.println( "<h1>Pipelines:</h1> <ul>");
			for ( RemusPipeline pipeline : app.pipelines.values() ) {
				out.println( "<li><h2><a href='/" + pipeline.id + "'>Pipeline " + pipeline.id + "</a></h2></li>" );
				out.println("<h3>CodeList</h3><ul>");
				for ( RemusApplet applet : pipeline.getMembers() ) {
					out.print( "<li><a href='" + applet.getPath() + "'>" + applet.getPath() + "</a>" );
					if ( applet.getInput().getInputType() == RemusPath.DynamicInput ) {
						out.print(" - Input Applet");
					}
					if ( applet.getInput().getInputType() == RemusPath.StaticInput ) {
						out.print(" - Static Applet");
					}
					out.println("</li><ul>");
					for ( RemusInstance appInst : applet.getInstanceList() ) {
						out.print( "<li><a href='" + applet.getPath() + "@/" + appInst.toString() + "'>" +
								appInst.toString() + "</a> - ");
						if ( applet.isInError(appInst) ) {
							out.print("ERROR"); 
						} else {
							if ( applet.isReady(appInst) ) {
								if ( applet.isComplete(appInst) ) {
									out.print( "Complete");
								} else {
									out.print( "Ready");									
								}
							} else {
								out.print( "Waiting");
							}
						}
						out.print( "</li>" );
					}
					out.println("</ul>");
				}
				out.println("</ul>");
			}
			out.println("</ul>");			
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {		
		RemusPath reqInfo = new RemusPath( app, req.getRequestURI() );

		if ( reqInfo.getView() == null ) {
			doGet_template( reqInfo, req, resp );
		} else if ( reqInfo.getView().compareTo("pipeline") == 0 ) {
			doGet_pipeline(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("attach") == 0 ) {
			doGet_attach(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("work") == 0 ) {
			doGet_work(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("submit") == 0 ) {
			doGet_submit( reqInfo, req, resp );
		} else if ( reqInfo.getView().compareTo("data") == 0 ) {
			doGet_data( reqInfo, req, resp );
		} else if ( reqInfo.getView().compareTo("reduce") == 0 ) {
			doGet_reduce(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("keys") == 0 ) {
			doGet_keys(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("instance") == 0 ) {
			doGet_instance(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("status") == 0 ) {
			doGet_status(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("config") == 0 ) {
			doGet_config(reqInfo, req, resp);
		}
	}


	private void doPost_alias(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		if( reqInfo.getInstance() != null ) {
			BufferedReader br = req.getReader();
			String curline = br.readLine();
			app.addAlias( new RemusInstance(reqInfo.getInstance()), curline );		
		}
	}

	private void doPost_work(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {

		String workerID = getWorkerID(req);
		if ( workerID != null ) {
			BufferedReader br = req.getReader();
			String curline = null;
			while ((curline=br.readLine())!= null ) {
				Map m = (Map)serializer.loads( curline );
				for ( Object key : m.keySet() ) {
					String instStr = (String)key;
					RemusInstance inst=new RemusInstance(instStr);
					List jobList = (List)m.get(key);
					RemusApplet applet = app.getApplet( reqInfo.getAppletPath() );
					for ( Object key2 : jobList ) {
						long jobID = Long.parseLong( key2.toString() );
						//TODO:add emit id count check
						app.getWorkManager().finishWork(workerID, applet, inst, (int)jobID, 0L);
					}						
				}
			}
		}
		resp.getWriter().print("\"OK\"");
	}


	private void doPost_error(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		String workerID = getWorkerID(req);
		if ( workerID != null ) {
			BufferedReader br = req.getReader();
			String curline = null;
			while ((curline=br.readLine())!= null ) {
				Map m = (Map)serializer.loads( curline );
				for ( Object key : m.keySet() ) {
					String instStr = (String)key;
					RemusInstance inst=new RemusInstance(instStr);
					Map jobErrors = (Map)m.get(key);
					RemusApplet applet = app.getApplet( reqInfo.getAppletPath() );
					for ( Object key2 : jobErrors.keySet() ) {
						long jobID = Long.parseLong( key2.toString() );
						app.getWorkManager().errorWork(workerID, applet, inst, (int)jobID, (String)jobErrors.get(key2) );
					}						
				}
			}
			resp.getWriter().print("\"OK\"");
		}
	}

	private void doPost_data(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
		if ( applet != null ) {
			if ( reqInfo.getInstance() != null) {
				RemusInstance inst = new RemusInstance(reqInfo.getInstance());
				String workerID = getWorkerID(req);
				if ( workerID != null ) {
					//TODO: make sure correct worker is returning assigned results before putting them in the database....
					app.getWorkManager().touchWorkerStatus( workerID );
					Set<Integer> out = applet.formatInput( reqInfo, req.getInputStream(), serializer );
					if ( out != null ) {
						for (int jobID : out ) {
							if ( !app.getWorkManager().hasWork( workerID, applet, inst, jobID ) ) {
								System.err.println("WRONG WORKER RETURNING RESULTS!!!");
							}
						}
					}
					resp.getWriter().print("\"OK\"");
				} else {
					if ( applet.getType() == RemusApplet.STORE ) {
						applet.formatInput( reqInfo, req.getInputStream(), serializer );
					}
				}
			}
		}
	}


	private void doPost_submit(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		RemusPipeline pipe = app.getPipeline( reqInfo.getPipeline() );
		if ( pipe != null ) {
			BufferedReader br = req.getReader();
			StringBuilder sb = new StringBuilder();
			String curline;
			while ( (curline=br.readLine()) != null ) {
				sb.append(curline);
			}
			Map objMap=(Map)serializer.loads(sb.toString());			
			for ( Object keyObj : objMap.keySet() ) {
				pipe.submit( (String)keyObj, objMap.get(keyObj) );
			}
			resp.getWriter().print("{\"submit\":\"OK\"}");
		}
	}

	private void doPost_instance(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
		if ( applet != null ) {
			BufferedReader br = req.getReader();
			String curline = br.readLine();	
			RemusInstance inst = applet.createInstance( curline );
			Map out = new HashMap();
			out.put("OK", inst.toString() );
			resp.getWriter().print( serializer.dumps(out) );
		}
	}

	private void doPost_attach(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
		if ( reqInfo.getInstance() != null ) {
			applet.getAttachStore().writeAttachment( reqInfo.getAppletPath() + "@attach", reqInfo.getInstance(), reqInfo.getKey(), req.getInputStream() );
		}
	}



	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		RemusPath reqInfo = new RemusPath(app, req.getPathInfo() );	
		if ( reqInfo.getView() == null ) {

		} else if ( reqInfo.getView().compareTo("alias") == 0  ) {
			doPost_alias(reqInfo, req, resp);
		} else 	if ( reqInfo.getView().compareTo("work") == 0 ) {
			doPost_work(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("error") == 0 ) {
			doPost_error(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("data") == 0 ) {
			doPost_data(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("submit") == 0) {
			doPost_submit(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("instance") == 0) {
			doPost_instance(reqInfo, req, resp);		
		} else if ( reqInfo.getView().compareTo("attach") == 0 ) {
			doPost_attach(reqInfo, req, resp);
		}
	}



	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		RemusPath reqInfo = new RemusPath(app, req.getRequestURI() );	
		PrintWriter out = resp.getWriter();
		if ( reqInfo.getPipeline() == null && reqInfo.getInstance() != null && reqInfo.getView().compareTo("pipeline") == 0 ) {
			//posting to root pipeline database to create a new pipeline
			BufferedReader br = req.getReader();
			StringBuilder sb = new StringBuilder();
			String curLine = null;
			while( (curLine=br.readLine())!= null ) {
				sb.append(curLine);
			}
			Object data = serializer.loads(sb.toString());
			app.putPipeline( reqInfo.getInstance(), data );			
		} else if ( reqInfo.getPipeline() != null && reqInfo.getApplet() != null && reqInfo.getView().compareTo("pipeline") == 0 ) {			
			//posting applet to pipleline
			RemusPipeline pipe = app.getPipeline( reqInfo.getPipeline() );
			if ( pipe != null ) {
				BufferedReader br = req.getReader();
				StringBuilder sb = new StringBuilder();
				String curLine = null;
				while( (curLine=br.readLine())!= null ) {
					sb.append(curLine);
				}
				Object data = serializer.loads(sb.toString());
				app.putApplet(pipe, reqInfo.getApplet(), data);
				out.println( "PUTTING APPLET: " + reqInfo.getPipeline() + " " + reqInfo.getApplet() );
			}
		} else if ( reqInfo.getPipeline() != null && reqInfo.getApplet() == null && reqInfo.getKey() == null && reqInfo.getView().compareTo("attach")==0) {
			//posting attachment to pipeline			
			AttachStore ds = app.getRootAttachStore();
			ds.writeAttachment("/" + reqInfo.getPipeline() + "@attach" , RemusInstance.STATIC_INSTANCE_STR, reqInfo.getInstance(), req.getInputStream() );
			out.println("PUTTING ATTACHMENT: " +  reqInfo.getPipeline() + " " + reqInfo.getKey() );			
		}
	}



	private void doDelete_instance(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		if ( reqInfo.getApplet() == null ) {
			if ( reqInfo.getInstance() != null ) {
				RemusInstance instance = new RemusInstance(reqInfo.getInstance());
				for ( RemusPipeline pipeline : app.pipelines.values() ) {
					pipeline.deleteInstance(instance);
				}
				try {
					app = new RemusApp(configMap );
					//workManage = new WorkManager(app);
				} catch (RemusDatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				resp.getWriter().print( "{\"delete\":\"OK\"}" );
			}
		} else if ( app.hasApplet( reqInfo.getAppletPath() ) ) {
			RemusApplet applet = app.getApplet(reqInfo.getAppletPath());
			if ( reqInfo.getInstance() != null  ) {
				applet.deleteInstance( new RemusInstance( reqInfo.getInstance()) );
				try {
					app = new RemusApp( configMap );
					//workManage = new WorkManager(app);
				} catch (RemusDatabaseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				resp.getWriter().print( "{\"delete\":\"OK\"}" );
			}
		} 		
	}	


	private void doDelete_pipeline(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		RemusPipeline pipe = app.pipelines.get(reqInfo.getPipeline());
		if ( pipe != null ) {
			RemusApplet applet = pipe.getApplet( reqInfo.getApplet() );
			if ( applet != null  ) {				
				app.deleteApplet( pipe, applet );
				//workManage = new WorkManager(app);
				resp.getWriter().println( "{\"result\":\"OK\"}" );
			} else {
				app.deletePipeline( pipe );
				//app = new RemusApp( configMap );
			}
		}
	}

	private void doDelete_error(RemusPath reqInfo, HttpServletRequest req, HttpServletResponse resp) throws IOException {
		if ( reqInfo.getApplet() == null ) {
			for ( RemusPipeline pipe : app.pipelines.values() ) {
				for ( RemusApplet app : pipe.getMembers() ) {
					for ( RemusInstance inst : app.getInstanceList() ) {
						app.deleteErrors( inst );
					}
				}
			}
		}
	}

	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		RemusPath reqInfo = new RemusPath(app, req.getRequestURI() );	

		if ( reqInfo.getView() == null ) {

		} else if ( reqInfo.getView().compareTo("instance") == 0 ) {
			doDelete_instance(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("pipeline") == 0 ) {
			doDelete_pipeline(reqInfo, req, resp);
		} else if ( reqInfo.getView().compareTo("error") == 0 ) {
			doDelete_error( reqInfo, req, resp);
		}

	}
}




