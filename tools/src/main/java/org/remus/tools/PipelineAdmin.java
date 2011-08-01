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
import org.remus.RemusInstance;
import org.remus.server.RemusApp;
import org.remus.server.RemusDatabaseException;
import org.remus.server.RemusPipelineImpl;
import org.remus.work.RemusAppletImpl;
import org.remusNet.JSON;
import org.remusNet.KeyValPair;
import org.remusNet.RemusAttach;
import org.remusNet.RemusDB;
import org.remusNet.thrift.AppletRef;

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

	public static void tableDump( RemusPipelineImpl pipe, String instance, File instDir ) throws IOException, TException  {
		System.err.println( "PIPELINE: " + pipe.getID() );

		File submitFile = new File(instDir, "@submit");
		FileOutputStream fsOS = new FileOutputStream(submitFile);
		AppletRef arSubmit = new AppletRef(pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit" );
		for ( KeyValPair kv : pipe.getApp().getRootDatastore().listKeyPairs(arSubmit) ) {
			Map subObj = (Map)kv.getValue();
			if ( instance.compareTo( (String)subObj.get("_instance")) == 0 ) {
				Map<String,Object> m = new HashMap<String,Object>();
				m.put(kv.getKey(), kv.getValue() );							
				fsOS.write( Long.toString(kv.getJobID()).getBytes() );
				fsOS.write( "\t".getBytes() );
				fsOS.write( Long.toString(kv.getEmitID()).getBytes() );
				fsOS.write( "\t".getBytes() );
				fsOS.write( JSON.dumps( m ).getBytes() );							
				fsOS.write( "\n".getBytes() );
			}
		}
		fsOS.close();

		File globalInstFile = new File(instDir, "@instance");
		FileOutputStream giOS = new FileOutputStream(globalInstFile);
		AppletRef arInstance = new AppletRef(pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance" );

		for ( KeyValPair kv : pipe.getApp().getRootDatastore().listKeyPairs(arInstance) ) {
			if ( instance.compareTo( kv.getKey() )  == 0 ) {
				Map<String,Object> m = new HashMap<String,Object>();
				m.put(kv.getKey(), kv.getValue() );							
				giOS.write( Long.toString(kv.getJobID()).getBytes() );
				giOS.write( "\t".getBytes() );
				giOS.write( Long.toString(kv.getEmitID()).getBytes() );
				giOS.write( "\t".getBytes() );
				giOS.write( JSON.dumps( m ).getBytes() );							
				giOS.write( "\n".getBytes() );
			}
		}
		giOS.close();

		for ( RemusAppletImpl applet : pipe.getMembers() ) {
			System.err.println( "Dumping: " + applet.getID() );

			File instanceFile = new File(instDir, applet.getID() + "@instance");
			FileOutputStream insOS = new FileOutputStream(instanceFile);
			for ( KeyValPair kv : pipe.getApp().getRootDatastore().listKeyPairs( arInstance) ) {
				if ( instance.compareTo( kv.getKey() )  == 0 ) {
					Map<String,Object> m = new HashMap<String,Object>();
					m.put(kv.getKey(), kv.getValue() );							
					insOS.write( Long.toString(kv.getJobID()).getBytes() );
					insOS.write( "\t".getBytes() );
					insOS.write( Long.toString(kv.getEmitID()).getBytes() );
					insOS.write( "\t".getBytes() );
					insOS.write( JSON.dumps( m ).getBytes() );							
					insOS.write( "\n".getBytes() );
				}
			}
			insOS.close();	


			File outFile = new File(instDir, applet.getID() + "@data" );
			FileOutputStream fos = new FileOutputStream(outFile);
			RemusDB ds = applet.getDataStore();
			String curKey = null;
			AppletRef ar = new AppletRef(applet.getPipeline().getID(), instance.toString(), applet.getID() );
			for ( KeyValPair kv : ds.listKeyPairs(ar) ) {
				Map<String,Object> m = new HashMap<String,Object>();
				m.put(kv.getKey(), kv.getValue() );							
				fos.write( Long.toString(kv.getJobID()).getBytes() );
				fos.write( "\t".getBytes() );
				fos.write( Long.toString(kv.getEmitID()).getBytes() );
				fos.write( "\t".getBytes() );
				fos.write( JSON.dumps( m ).getBytes() );							
				fos.write( "\n".getBytes() );

				if ( curKey == null || kv.getKey().compareTo( curKey ) != 0 ) {
					curKey = kv.getKey();
					RemusAttach att = applet.getAttachStore();
					
					for ( String name : att.listAttachments( ar, curKey) ) {
						File appletDir = new File( instDir, applet.getID() );
						File keyDir = new File( appletDir, curKey );
						if ( !keyDir.exists() ) {
							keyDir.mkdirs();
						}
						FileOutputStream attOS = new FileOutputStream( new File(keyDir, name ) );
						InputStream is = att.readAttachement( ar, curKey, name);
						byte [] buffer = new byte[4048];
						int len;
						while ( ((len=is.read(buffer)) > 0 ) ) {
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




	public static void loadTable( RemusPipelineImpl pipe, RemusInstance instance, File loadDir ) throws IOException {
		for ( File stackFile : loadDir.listFiles() ) {
			if ( !stackFile.isDirectory() ) {
				if ( stackFile.getName().compareTo("@submit") == 0 ) {
					loadTableFile( pipe.getDataStore(), stackFile, "/@submit", RemusInstance.STATIC_INSTANCE_STR );
				} else if ( stackFile.getName().compareTo("@instance") == 0 ) {
					loadTableFile( pipe.getDataStore(), stackFile, "/@instance", RemusInstance.STATIC_INSTANCE_STR );
				} else if ( stackFile.getName().endsWith("@data") ) {
					String appletName = stackFile.getName().replace("@data", "");
					loadTableFile( pipe.getDataStore(), stackFile, "/" + pipe.getID() + "/" + appletName, instance.toString() );
				}
			}
		}
	}

	public static void main(String []args) throws FileNotFoundException, IOException, RemusDatabaseException, TException {
		Properties prop = new Properties();
		prop.load( new FileInputStream( new File( args[0] ) ) );


		String outDirPath = "out";
		File outDir = new File(outDirPath);
		if ( !outDir.exists() ) {
			outDir.mkdir();			
		}

		try {
			RemusApp app = new RemusApp( prop );
			String cmd = null;
			if ( args.length > 1 )
				cmd = args[1];

			if ( cmd == null || cmd.compareTo("list") == 0 ) {
				if ( args.length > 2 ) {
					Set<RemusInstance> outSet = new HashSet<RemusInstance>(); 
					RemusPipelineImpl pipe = app.getPipeline(args[2]);
					if ( pipe != null ) {
						for ( RemusAppletImpl applet : pipe.getMembers() ) {
							for ( RemusInstance inst : applet.getInstanceList() ) {
								outSet.add(inst);
							}
						}
						for ( RemusInstance inst : outSet ) {
							System.out.println( inst.toString() );
						}
					}
				} else {
					for ( RemusPipelineImpl pipeline : app.getPipelines() ) {
						System.out.println( pipeline.getID() );
					}
				}
			} else {
				if ( cmd.compareTo("dump") == 0 && args.length > 2 ) {
					String pipeline = args[2];
					String inst = args[3];
					RemusPipelineImpl pipe = app.getPipeline(pipeline);

					if ( inst.compareTo("--all") == 0 ) {
						Set<RemusInstance> instSet = new HashSet<RemusInstance>();
						for ( RemusAppletImpl applet : pipe.getMembers() ) {
							instSet.addAll( applet.getInstanceList() );
						}
						for ( RemusInstance instance : instSet ) {
							File instDir = new File( outDir, instance.toString() );
							if (!instDir.exists())
								instDir.mkdirs();					
							tableDump( pipe, instance.toString(), instDir );							
						}
					} else {					
						RemusInstance instance = new RemusInstance(inst);
						File instDir = new File( outDir, inst );
						if (!instDir.exists())
							instDir.mkdirs();					
						tableDump( pipe, instance.toString(), instDir );
					}

				} else if ( cmd.compareTo("load") == 0 && args.length > 2) {
					String pipeline = args[2];
					String srcDirPath = args[3];

					RemusPipelineImpl pipe = app.getPipeline(pipeline);

					File srcDir = new File( srcDirPath );
					RemusInstance instance = new RemusInstance( srcDir.getName() );
					loadTable( pipe, instance, srcDir );
				}
			}
		} finally {

		} 	
	}

}
