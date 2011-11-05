package org.remus.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.core.RemusApp;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PeerManager;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;

/**
 * Pipeline administration tool. Primarily for dumping and loading pipeline
 * instances.
 * @author kellrott
 *
 */

public class PipelineAdmin {

	static public String [] storeViews = { "@data" }; 
	static public String [] fileViews = { "@attach" }; 

	static public String [] allViews = { "@data", "@done", "@instance" }; 

	public static void tableDump(RemusDB datastore, RemusPipeline pipe, 
			String instance, File instDir ) throws IOException, TException, NotImplemented, RemusDatabaseException  {
		System.err.println("PIPELINE: " + pipe.getID());

		File submitFile = new File(instDir, "@submit");
		FileOutputStream fsOS = new FileOutputStream(submitFile);
		AppletRef arSubmit = new AppletRef(pipe.getID(), 
				RemusInstance.STATIC_INSTANCE_STR, Constants.SUBMIT_APPLET);
		for (KeyValPair kv : datastore.listKeyPairs(arSubmit)) {
			Map subObj = (Map) kv.getValue();
			if (instance.compareTo((String) subObj.get("_instance")) == 0) {
				Map<String, Object> m = new HashMap<String, Object>();
				m.put(kv.getKey(), kv.getValue());
				fsOS.write(Long.toString(kv.getJobID()).getBytes());
				fsOS.write("\t".getBytes());
				fsOS.write(Long.toString(kv.getEmitID()).getBytes());
				fsOS.write("\t".getBytes());
				fsOS.write(JSON.dumps(m).getBytes());
				fsOS.write("\n".getBytes());
			}
		}
		fsOS.close();

		File globalInstFile = new File(instDir, "@instance");
		FileOutputStream giOS = new FileOutputStream(globalInstFile);
		AppletRef arInstance = new AppletRef(pipe.getID(),
				RemusInstance.STATIC_INSTANCE_STR, Constants.INSTANCE_APPLET);

		for (KeyValPair kv : datastore.listKeyPairs(arInstance)) {
			if (instance.compareTo(kv.getKey())  == 0) {
				Map<String, Object> m = new HashMap<String, Object>();
				m.put(kv.getKey(), kv.getValue());
				giOS.write(Long.toString(kv.getJobID()).getBytes());
				giOS.write("\t".getBytes());
				giOS.write(Long.toString(kv.getEmitID()).getBytes());
				giOS.write("\t".getBytes());
				giOS.write(JSON.dumps(m).getBytes());
				giOS.write("\n".getBytes());
			}
		}
		giOS.close();

		for (String appletName : pipe.getMembers()) {
			System.err.println("Dumping: " + appletName);
			RemusApplet applet = pipe.getApplet(appletName);

			File instanceFile = new File(instDir, applet.getID() + Constants.INSTANCE_APPLET);
			FileOutputStream insOS = new FileOutputStream(instanceFile);
			for (KeyValPair kv : datastore.listKeyPairs(arInstance)) {
				if (instance.compareTo(kv.getKey())  == 0) {
					Map<String, Object> m = new HashMap<String, Object>();
					m.put(kv.getKey(), kv.getValue());
					insOS.write(Long.toString(kv.getJobID()).getBytes());
					insOS.write("\t".getBytes());
					insOS.write(Long.toString(kv.getEmitID()).getBytes());
					insOS.write("\t".getBytes());
					insOS.write(JSON.dumps(m).getBytes());
					insOS.write("\n".getBytes());
				}
			}
			insOS.close();	


			File outFile = new File(instDir, applet.getID() + "@data" );
			FileOutputStream fos = new FileOutputStream(outFile);
			RemusDB ds = applet.getDataStore();
			String curKey = null;
			AppletRef ar = new AppletRef(pipe.getID(),
					instance.toString(), applet.getID());
			for (KeyValPair kv : ds.listKeyPairs(ar)) {
				Map<String, Object> m = new HashMap<String, Object>();
				m.put(kv.getKey(), kv.getValue());
				fos.write(Long.toString(kv.getJobID()).getBytes());
				fos.write("\t".getBytes());
				fos.write(Long.toString(kv.getEmitID()).getBytes());
				fos.write("\t".getBytes());
				fos.write(JSON.dumps(m).getBytes());
				fos.write("\n".getBytes());

				if (curKey == null || kv.getKey().compareTo(curKey) != 0) {
					curKey = kv.getKey();
					RemusAttach att = applet.getAttachStore();

					for (String name : att.listAttachments(ar, curKey)) {
						File appletDir = new File(instDir, applet.getID());
						File keyDir = new File(appletDir, curKey);
						if (!keyDir.exists()) {
							keyDir.mkdirs();
						}
						FileOutputStream attOS = 
							new FileOutputStream(new File(keyDir, name));
						InputStream is = att.readAttachment(ar, curKey, name);
						byte [] buffer = new byte[4048];
						int len;
						while (((len = is.read(buffer)) > 0)) {
							attOS.write(buffer, 0, len);
						}
						attOS.close();
						is.close();
					}
				}
			}			
			fos.close();	
		}
	}



	public static void loadTableFile( RemusDB store, File storeFile, String tablePath, String instance ) throws IOException {
		System.err.println("LOADING: " + storeFile.toString() );
		BufferedReader br = new BufferedReader( new FileReader( storeFile ) );
		String curline = null;
		//BUG:MAKE THIS WORK AGAIN!!!
		/*
		AppletRef ap = new AppletRef()
		while ((curline=br.readLine()) != null) {
			String [] tmp = curline.split("\t");
			long jobID = Long.parseLong( tmp[0] );
			long emitID = Long.parseLong( tmp[1] );
			Map m = (Map)JSON.loads( tmp[2] );
			for ( Object keyObj : m.keySet() ) {
				String key = (String)keyObj;
				store.add(tablePath, instance, jobID, emitID, key, m.get(key) );
			}
		}
		 */
		br.close();
	}




	public static void loadTable( RemusDB datastore, RemusPipeline pipe, RemusInstance instance, File loadDir ) throws IOException {
		for (File stackFile : loadDir.listFiles()) {
			if (!stackFile.isDirectory()) {
				if (stackFile.getName().compareTo("@submit") == 0) {
					loadTableFile(datastore, 
							stackFile, Constants.SUBMIT_APPLET, 
							RemusInstance.STATIC_INSTANCE_STR);
				} else if (stackFile.getName().compareTo("@instance") == 0) {
					loadTableFile(datastore, 
							stackFile, Constants.INSTANCE_APPLET, 
							RemusInstance.STATIC_INSTANCE_STR);
				} else if (stackFile.getName().endsWith("@data")) {
					String appletName = 
						stackFile.getName().replace("@data", "");
					loadTableFile(datastore, 
							stackFile, "/" + pipe.getID() + "/" + appletName, 
							instance.toString());
				}
			}
		}
	}

	public static void main(String []args) throws Exception {
		Map params = (Map) JSONValue.parse( new FileReader(new File(args[0])));


		String outDirPath = "out";
		File outDir = new File(outDirPath);
		if (!outDir.exists()) {
			outDir.mkdir();			
		}

		try {
			PluginManager plugMan = new PluginManager(params);
			plugMan.start();
			PeerManager pm = plugMan.getPeerManager();
			RemusApp app = new RemusApp((RemusDB) pm.getPeer(pm.getDataServer()),
					(RemusAttach) pm.getPeer(pm.getAttachStore()));
			String cmd = null;
			if (args.length > 1) {
				cmd = args[1];
			}

			if (cmd == null || cmd.compareTo("list") == 0) {
				if (args.length > 2) {
					Set<RemusInstance> outSet = new HashSet<RemusInstance>(); 
					RemusPipeline pipe = app.getPipeline(args[2]);
					if (pipe != null) {
						for (String appletName : pipe.getMembers()) {
							RemusApplet applet = pipe.getApplet(appletName);
							for (RemusInstance inst : applet.getInstanceList()) {
								outSet.add(inst);
							}
						}
						for (RemusInstance inst : outSet) {
							System.out.println(inst.toString());
						}
					}
				} else {
					for (String pipelineName : app.getPipelines()) {
						System.out.println(pipelineName);
					}
				}
			} else {
				if (cmd.compareTo("dump") == 0 && args.length > 2) {
					String pipeline = args[2];
					String inst = args[3];
					RemusPipeline pipe = app.getPipeline(pipeline);

					if (inst.compareTo("--all") == 0) {
						Set<RemusInstance> instSet = new HashSet<RemusInstance>();
						for (String appletName : pipe.getMembers()) {
							RemusApplet applet = pipe.getApplet(appletName);
							instSet.addAll(applet.getInstanceList());
						}
						for (RemusInstance instance : instSet) {
							File instDir = new File(outDir, instance.toString());
							if (!instDir.exists()) {
								instDir.mkdirs();
							}
							tableDump((RemusDB) pm.getPeer(pm.getDataServer()), pipe, instance.toString(), instDir);
						}
					} else {					
						RemusInstance instance = new RemusInstance(inst);
						File instDir = new File(outDir, inst);
						if (!instDir.exists()) {
							instDir.mkdirs();	
						}
						tableDump((RemusDB) pm.getPeer(pm.getDataServer()), pipe, instance.toString(), instDir);
					}

				} else if (cmd.compareTo("load") == 0 && args.length > 2) {
					String pipeline = args[2];
					String srcDirPath = args[3];

					RemusPipeline pipe = app.getPipeline(pipeline);

					File srcDir = new File(srcDirPath);
					RemusInstance instance = new RemusInstance(srcDir.getName());
					loadTable((RemusDB) pm.getPeer(pm.getDataServer()), pipe, instance, srcDir);
				}
			}
		} finally {

		} 	
	}

}
