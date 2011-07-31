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

import org.mpstore.AttachStore;
import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.mpstore.Serializer;
import org.mpstore.impl.JsonSerializer;
import org.remus.RemusInstance;
import org.remus.server.RemusApp;
import org.remus.server.RemusDatabaseException;
import org.remus.server.RemusPipelineImpl;
import org.remus.work.RemusAppletImpl;

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

	public static void tableDump( RemusPipelineImpl pipe, Serializer serializer, String instance, File instDir ) throws IOException  {
		System.err.println( "PIPELINE: " + pipe.getID() );

		File submitFile = new File(instDir, "@submit");
		FileOutputStream fsOS = new FileOutputStream(submitFile);
		for ( KeyValuePair kv : pipe.getApp().getRootDatastore().listKeyPairs( "/" + pipe.getID() + "/@submit", RemusInstance.STATIC_INSTANCE_STR) ) {
			Map subObj = (Map)kv.getValue();
			if ( instance.compareTo( (String)subObj.get("_instance")) == 0 ) {
				Map<String,Object> m = new HashMap<String,Object>();
				m.put(kv.getKey(), kv.getValue() );							
				fsOS.write( Long.toString(kv.getJobID()).getBytes() );
				fsOS.write( "\t".getBytes() );
				fsOS.write( Long.toString(kv.getEmitID()).getBytes() );
				fsOS.write( "\t".getBytes() );
				fsOS.write( serializer.dumps( m ).getBytes() );							
				fsOS.write( "\n".getBytes() );
			}
		}
		fsOS.close();

		File globalInstFile = new File(instDir, "@instance");
		FileOutputStream giOS = new FileOutputStream(globalInstFile);
		for ( KeyValuePair kv : pipe.getApp().getRootDatastore().listKeyPairs( "/" + pipe.getID() + "/@instance", RemusInstance.STATIC_INSTANCE_STR) ) {
			if ( instance.compareTo( kv.getKey() )  == 0 ) {
				Map<String,Object> m = new HashMap<String,Object>();
				m.put(kv.getKey(), kv.getValue() );							
				giOS.write( Long.toString(kv.getJobID()).getBytes() );
				giOS.write( "\t".getBytes() );
				giOS.write( Long.toString(kv.getEmitID()).getBytes() );
				giOS.write( "\t".getBytes() );
				giOS.write( serializer.dumps( m ).getBytes() );							
				giOS.write( "\n".getBytes() );
			}
		}
		giOS.close();

		for ( RemusAppletImpl applet : pipe.getMembers() ) {
			System.err.println( "Dumping: " + applet.getID() );

			File instanceFile = new File(instDir, applet.getID() + "@instance");
			FileOutputStream insOS = new FileOutputStream(instanceFile);
			for ( KeyValuePair kv : pipe.getApp().getRootDatastore().listKeyPairs( applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR) ) {
				if ( instance.compareTo( kv.getKey() )  == 0 ) {
					Map<String,Object> m = new HashMap<String,Object>();
					m.put(kv.getKey(), kv.getValue() );							
					insOS.write( Long.toString(kv.getJobID()).getBytes() );
					insOS.write( "\t".getBytes() );
					insOS.write( Long.toString(kv.getEmitID()).getBytes() );
					insOS.write( "\t".getBytes() );
					insOS.write( serializer.dumps( m ).getBytes() );							
					insOS.write( "\n".getBytes() );
				}
			}
			insOS.close();	


			File outFile = new File(instDir, applet.getID() + "@data" );
			FileOutputStream fos = new FileOutputStream(outFile);
			MPStore ds = applet.getDataStore();
			String curKey = null;
			for ( KeyValuePair kv : ds.listKeyPairs(applet.getPath(), instance.toString() ) ) {
				Map<String,Object> m = new HashMap<String,Object>();
				m.put(kv.getKey(), kv.getValue() );							
				fos.write( Long.toString(kv.getJobID()).getBytes() );
				fos.write( "\t".getBytes() );
				fos.write( Long.toString(kv.getEmitID()).getBytes() );
				fos.write( "\t".getBytes() );
				fos.write( serializer.dumps( m ).getBytes() );							
				fos.write( "\n".getBytes() );

				if ( curKey == null || kv.getKey().compareTo( curKey ) != 0 ) {
					curKey = kv.getKey();
					AttachStore att = applet.getAttachStore();
					for ( String name : att.listAttachment( applet.getPath(), instance.toString(), curKey) ) {
						File appletDir = new File( instDir, applet.getID() );
						File keyDir = new File( appletDir, curKey );
						if ( !keyDir.exists() ) {
							keyDir.mkdirs();
						}
						FileOutputStream attOS = new FileOutputStream( new File(keyDir, name ) );
						InputStream is = att.readAttachement( applet.getPath(), instance.toString(), curKey, name);
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



	public static void loadTableFile( MPStore store, Serializer serializer, File storeFile, String tablePath, String instance ) throws IOException {
		System.err.println("LOADING: " + storeFile.toString() );
		BufferedReader br = new BufferedReader( new FileReader( storeFile ) );
		String curline = null;
		while ((curline=br.readLine()) != null) {
			String [] tmp = curline.split("\t");
			long jobID = Long.parseLong( tmp[0] );
			long emitID = Long.parseLong( tmp[1] );
			Map m = (Map)serializer.loads( tmp[2] );
			for ( Object keyObj : m.keySet() ) {
				String key = (String)keyObj;
				store.add(tablePath, instance, jobID, emitID, key, m.get(key) );
			}
		}
		br.close();
	}




	public static void loadTable( RemusPipelineImpl pipe, Serializer serializer, RemusInstance instance, File loadDir ) throws IOException {
		for ( File stackFile : loadDir.listFiles() ) {
			if ( !stackFile.isDirectory() ) {
				if ( stackFile.getName().compareTo("@submit") == 0 ) {
					loadTableFile( pipe.getDataStore(), serializer, stackFile, "/@submit", RemusInstance.STATIC_INSTANCE_STR );
				} else if ( stackFile.getName().compareTo("@instance") == 0 ) {
					loadTableFile( pipe.getDataStore(), serializer, stackFile, "/@instance", RemusInstance.STATIC_INSTANCE_STR );
				} else if ( stackFile.getName().endsWith("@data") ) {
					String appletName = stackFile.getName().replace("@data", "");
					loadTableFile( pipe.getDataStore(), serializer, stackFile, "/" + pipe.getID() + "/" + appletName, instance.toString() );
				}
			}
		}
	}

	public static void main(String []args) throws FileNotFoundException, IOException, RemusDatabaseException {
		Properties prop = new Properties();
		prop.load( new FileInputStream( new File( args[0] ) ) );


		String outDirPath = "out";
		File outDir = new File(outDirPath);
		if ( !outDir.exists() ) {
			outDir.mkdir();			
		}

		try {
			Serializer serializer = new JsonSerializer();
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
							tableDump( pipe, serializer, instance.toString(), instDir );							
						}
					} else {					
						RemusInstance instance = new RemusInstance(inst);
						File instDir = new File( outDir, inst );
						if (!instDir.exists())
							instDir.mkdirs();					
						tableDump( pipe, serializer, instance.toString(), instDir );
					}

				} else if ( cmd.compareTo("load") == 0 && args.length > 2) {
					String pipeline = args[2];
					String srcDirPath = args[3];

					RemusPipelineImpl pipe = app.getPipeline(pipeline);

					File srcDir = new File( srcDirPath );
					RemusInstance instance = new RemusInstance( srcDir.getName() );
					loadTable( pipe, serializer, instance, srcDir );
				}
			}
		} finally {

		} 	
	}

}
