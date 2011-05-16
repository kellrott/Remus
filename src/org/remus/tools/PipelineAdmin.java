package org.remus.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.codec.binary.Base64InputStream;
import org.mpstore.JsonSerializer;
import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.mpstore.Serializer;
import org.remus.RemusApp;
import org.remus.RemusDatabaseException;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.work.RemusApplet;

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

	public static void main(String []args) throws FileNotFoundException, IOException, RemusDatabaseException {
		Properties prop = new Properties();
		prop.load( new FileInputStream( new File( args[0] ) ) );

		try {
			Serializer serializer = new JsonSerializer();
			RemusApp app = new RemusApp( prop );
			String cmd = null;
			if ( args.length > 1 )
				cmd = args[1];

			if ( cmd == null || cmd.compareTo("list") == 0 ) {
				if ( args.length > 2 ) {
					Set<RemusInstance> outSet = new HashSet<RemusInstance>(); 
					RemusPipeline pipe = app.getPipeline(args[2]);
					if ( pipe != null ) {
						for ( RemusApplet applet : pipe.getMembers() ) {
							for ( RemusInstance inst : applet.getInstanceList() ) {
								outSet.add(inst);
							}
						}
						for ( RemusInstance inst : outSet ) {
							System.out.println( inst.toString() );
						}
					}
				} else {
					for ( RemusPipeline pipeline : app.getPipelines() ) {
						System.out.println( pipeline.getID() );
					}
				}
			} else {
				if ( cmd.compareTo("dump") == 0 && args.length > 2 ) {
					String pipeline = args[2];
					String inst = args[3];
					RemusPipeline pipe = app.getPipeline(pipeline);
					RemusInstance instance = new RemusInstance(inst);

					for ( RemusApplet applet : pipe.getMembers() ) {
						System.out.println( "===" + applet.getPath() + "@data" );
						MPStore ds = applet.getDataStore();
						for ( KeyValuePair kv : ds.listKeyPairs(applet.getPath() + "@data", instance.toString() ) ) {
							Map m = new HashMap();
							m.put(kv.getKey(), kv.getValue() );
							System.out.println( Long.toString(kv.getJobID()) + "\t" + Long.toString(kv.getEmitID()) + "\t" + serializer.dumps( m ) );							
						}
						for ( String outpath : applet.getOutputs() ) {
							System.out.println( "===" + applet.getPath() + "." + outpath + "@data" );
							for ( KeyValuePair kv : ds.listKeyPairs(applet.getPath() + "." + outpath + "@data", instance.toString() ) ) {
								Map m = new HashMap();
								m.put(kv.getKey(), kv.getValue() );
								System.out.println( Long.toString(kv.getJobID()) + "\t" + Long.toString(kv.getEmitID()) + "\t" + serializer.dumps( m ) );							
							}
						}
					}

				} else if ( cmd.compareTo("load") == 0 && args.length > 2) {
					/*
					BufferedReader br = new BufferedReader( new FileReader(args[2]));
					RemusPath curPath = null;
					String curline = null;
					while ((curline=br.readLine()) != null) {
						if (curline.startsWith("==")) {
							curPath = new RemusPath( app, curline.substring(2) );
							System.out.println( curPath.getViewPath() + "\t" + curPath.getInstance() );
						} else {
							String [] tmp = curline.split("\t");
							long jobID = Long.parseLong( tmp[0] );
							long emitID = Long.parseLong( tmp[1] );
							if ( tmp[2].compareTo("===") == 0) {
								String key = tmp[3];
								StringBuilder sb = new StringBuilder();
								boolean reading = true;
								do {
									String iLine = br.readLine();
									if ( iLine == null || iLine.compareTo("===") == 0) {
										reading = false;
									} else {
										sb.append( iLine );
									}
								} while (reading);
								ByteArrayInputStream bis = new ByteArrayInputStream( sb.toString().getBytes() );
								Base64InputStream is = new Base64InputStream(bis);
								//store.writeAttachment(curPath.getViewPath(), curPath.getInstance(), key, is);
								is.close();
							} else {
								Map m = (Map)serializer.loads( tmp[2] );
								for ( Object keyObj : m.keySet() ) {
									String key = (String)keyObj;
									store.add(curPath.getViewPath(), curPath.getInstance(), jobID, emitID, key, m.get(key) );
								}
							}
						}
					}
				} else if ( cmd.compareTo("del") == 0 && args.length > 2 ) {
					String inst = args[2];
					for (Object pathObj : store.get("/@pipeline", RemusInstance.STATIC_INSTANCE_STR, inst) ) {
						String path = (String)pathObj;
						for (String view : allViews ) {
							store.delete(path, inst);
						}
						store.delete( path + "@instance", RemusInstance.STATIC_INSTANCE_STR, inst);
					}
					store.delete("/@pipeline", RemusInstance.STATIC_INSTANCE_STR, inst);
				}
					 */
				}
			}
		} finally {

		} 	
	}

}
