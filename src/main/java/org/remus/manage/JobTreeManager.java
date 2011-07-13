package org.remus.manage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.serverNodes.BaseNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobTreeManager implements WorkAgent {

	final static public String JOBTREE_SERVER = "org.remus.manage.JobTreeManager.server";	
	private WorkManager parent;

	/***
	 * The list of code types that can be sent to the JobTreeManager
	 * 
	 */
	private List<String> codeTypes = Arrays.asList("python", "GenePatternTree");

	private Logger logger;

	private String server;
	private URL serverURL;

	@Override
	public void workPoll() {

		logger.info("JobTreeServer work poll contacting server: " + serverURL );

		URL jobURL = null;
		try {
			 jobURL = new URL( serverURL.toString() + "/jobID" );
		} catch (MalformedURLException e1) {
			return;
		}
		WorkStatus workStack = parent.requestWorkStack(this, codeTypes);

		if ( workStack != null ) {
			try {
				HttpURLConnection conn = (HttpURLConnection)jobURL.openConnection();
				//conn.setDoInput(true);
				conn.setDoOutput(true);
				//conn.setRequestMethod("PUT");
				//OutputStreamWriter out = new OutputStreamWriter(
				//		conn.getOutputStream());
				//out.write(workStack.getInstance().toString());
				//out.flush();
				//out.close();
				BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				String id = br.readLine();
				logger.info("JobTreeServer JobID: " + id);
				
				logger.info("JobType: " + workStack.getApplet().getType() );
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			parent.returnWorkStack(workStack);
		}

	}


	@Override
	public BaseNode getChild(String name) {
		return null;
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {

		try {
			os.write( "JobTree module".getBytes() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void init(WorkManager parent) {
		this.parent = parent;
		server = (String) parent.app.getParams().get(JOBTREE_SERVER);
		try {
			serverURL = new URL( server );
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger = LoggerFactory.getLogger(JobTreeManager.class);

	}

	@Override
	public String getName() {
		return "JobTreeManager";
	}




}
