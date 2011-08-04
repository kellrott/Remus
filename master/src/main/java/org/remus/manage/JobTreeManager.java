package org.remus.manage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.remus.core.BaseNode;
import org.remus.core.WorkAgent;
import org.remus.core.WorkManager;
import org.remus.core.WorkStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;


public class JobTreeManager implements WorkAgent {

	final static public String JOBTREE_SERVER = "org.remus.manage.JobTreeManager.server";	
	private WorkManager parent;

	/***
	 * The list of code types that can be sent to the JobTreeManager
	 * 
	 */
	private List<String> codeTypes = Arrays.asList("python", "GenePattern");

	private Logger logger;

	private String server;
	private URL serverURL;

	
	WatcherThread watcher;
	
	@Override
	public void init(WorkManager parent) {
		this.parent = parent;
		server = (String) parent.getParams().get(JOBTREE_SERVER);
		try {
			serverURL = new URL( server );
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger = LoggerFactory.getLogger(JobTreeManager.class);
		watcher = new WatcherThread();
	}

	@Override
	public String getName() {
		return "JobTreeManager";
	}


	class JobTreeJob {
		WorkStatus work;
		String jobID;
		JobTreeJob( WorkStatus work ) {
			this.work = work;
		}

		public void submit() throws IOException {
			URL jobURL = new URL( serverURL.toString() + "/jobID" );
			HttpURLConnection conn = (HttpURLConnection)jobURL.openConnection();
			conn.setDoOutput(true);
			BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String id = br.readLine();
			logger.info("JobTreeServer JobID: " + id);
			logger.info("JobType: " + work.getApplet().getType() );

			/*
			 * Send job parameters
			 */

			URL subURL = new URL( serverURL.toString() + "/job/" + id );				
			logger.info("Submitting job at: " + subURL);				
			HttpURLConnection subConn = (HttpURLConnection)subURL.openConnection();
			subConn.setRequestMethod("POST");
			subConn.setDoOutput(true);
			subConn.setDoInput(true);
			subConn.setUseCaches(false);
			subConn.setAllowUserInteraction(false);

			OutputStreamWriter out = new OutputStreamWriter(
					subConn.getOutputStream());

			Map<String,String> subData = new HashMap<String,String>();
			subData.put("instance", work.getInstance().toString() );
			out.write(  JSONValue.toJSONString(subData) );
			out.flush();
			out.close();
			InputStream subIn = subConn.getInputStream();
			Reader reader = new InputStreamReader(subIn);
			reader.close();			
		}

		public boolean jobDone() {
			// TODO Auto-generated method stub
			
			return false;
		}

	}



	class WatcherThread extends Thread {
		List <JobTreeJob> jobList;
		boolean quit;
		
		public WatcherThread() {
			quit = false;
			jobList = new LinkedList<JobTreeJob>();
		}

		public void addJob(JobTreeJob job) {
			synchronized (jobList) {
				jobList.add(job);
			}
		}
		
		
		@Override
		public void run() {
			while (!quit) {
				synchronized (jobList) {
					List <JobTreeJob> removeList = new LinkedList<JobTreeJob>();
					for ( JobTreeJob job : jobList ) {
						if ( job.jobDone() ) {
							removeList.add(job);
						}
					}
					for ( JobTreeJob job : removeList ) {
						removeList.remove(job);
						parent.completeWorkStack(job.work);
						logger.info( "JOB Done" + job.jobID);
						
					}
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
					quit = true;
				}
			}
			
		}

	}


	@Override
	public void workPoll() {

		logger.info("JobTreeServer work poll contacting server: " + serverURL );

		JSONParser json = new JSONParser();
		WorkStatus workStack = parent.requestWorkStack(this, codeTypes);

		if ( workStack != null ) {
			try {
				JobTreeJob job = new JobTreeJob(workStack);
				job.submit();
				watcher.addJob(job);
			} catch (IOException e) {
				logger.error("Unable to start work");
				parent.returnWorkStack(workStack);
			}
		}

	}


	@Override
	public boolean syncWorkPoll(WorkStatus work) {
		return false;
	}

}
