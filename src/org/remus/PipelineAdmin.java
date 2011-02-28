package org.remus;

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
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64InputStream;
import org.mpstore.JsonSerializer;
import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.mpstore.Serializer;

public class PipelineAdmin {

	static public String [] storeViews = { "@data" }; 
	static public String [] fileViews = { "@attach" }; 

	static public String [] allViews = { "@data", "@done", "@instance" }; 

	public static void main(String []args) throws FileNotFoundException, IOException, RemusDatabaseException {
		Properties prop = new Properties();
		prop.load( new FileInputStream( new File( args[0] ) ) );

		try {
			String mpStore = prop.getProperty(RemusApp.configStore);
			String workDir = prop.getProperty(RemusApp.configWork);
			String srcDir  = prop.getProperty(RemusApp.configSource);
			Serializer serializer = new JsonSerializer();
			Class<?> mpClass = Class.forName(mpStore);			
			MPStore store = (MPStore) mpClass.newInstance();
			store.init(serializer, workDir);			
			RemusApp app = new RemusApp(new File(srcDir), store);
			String cmd = null;
			if ( args.length > 1 )
				cmd = args[1];

			if ( cmd == null || cmd.compareTo("list") == 0 ) {
				if ( args.length > 2 ) {
					for (String inst : store.listKeys(args[2] + "@instance", RemusInstance.STATIC_INSTANCE_STR ) ) {
						System.out.println( inst );
					}
				} else {
					for (String pipeline : store.listKeys("/@pipeline", RemusInstance.STATIC_INSTANCE_STR) ) {
						System.out.println( pipeline );
					}
				}
			} else {
				if ( cmd.compareTo("dump") == 0 && args.length > 2 ) {
					String inst = args[2];
					System.out.println("==/@pipeline/" + RemusInstance.STATIC_INSTANCE_STR);
					int i = 0;
					for (Object pathObj : store.get("/@pipeline", RemusInstance.STATIC_INSTANCE_STR, inst) ) {
						Map m = new HashMap();
						m.put(inst, pathObj );
						System.out.println( Integer.toString(i) + "\t0\t" + serializer.dumps( m ) );						
					}
					for (Object pathObj : store.get("/@pipeline", RemusInstance.STATIC_INSTANCE_STR, inst) ) {
						String path = (String)pathObj;						
						System.out.println( "==" + path + "@instance/" + RemusInstance.STATIC_INSTANCE_STR );
						for ( KeyValuePair kv : store.listKeyPairs(path + "@instance", RemusInstance.STATIC_INSTANCE_STR) ) {
							if ( kv.getKey().compareTo(inst) == 0) {
								Map m = new HashMap();
								m.put(kv.getKey(), kv.getValue() );
								System.out.println( Long.toString(kv.getJobID()) + "\t" + Long.toString(kv.getEmitID()) + "\t" + serializer.dumps( m ) );
							}
						}						
						for ( String view : storeViews ) {
							System.out.println( "==" + path + view + "/" + inst);
							for ( KeyValuePair kv : store.listKeyPairs(path + view, inst) ) {
								Map m = new HashMap();
								m.put(kv.getKey(), kv.getValue() );
								System.out.println( Long.toString(kv.getJobID()) + "\t" + Long.toString(kv.getEmitID()) + "\t" + serializer.dumps( m ) );
							}
						}
						for ( String view : fileViews ) {
							if ( store.keyCount(path + view, inst, 1) > 0 ) {
								System.out.println( "==" + path + view + "/" + inst);
							}
							int j = 0;
							for ( String key : store.listKeys(path + view, inst) ) {
								System.out.println( Integer.toString(j) +"\t0\t===\t" + key );
								try {
									InputStream in = store.readAttachement(path + view, inst, key);
									Base64InputStream bis = new Base64InputStream(in,true);
									BufferedReader br = new BufferedReader( new InputStreamReader( bis ) );
									String curline = null;
									while ((curline=br.readLine())!=null) {
										System.out.println( curline );
									}
									br.close();
								} catch (IOException e) {									
								}
								System.out.println("===");
								j++;
							}							
						}
					}
				} else if ( cmd.compareTo("load") == 0 && args.length > 2) {
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
								store.writeAttachment(curPath.getViewPath(), curPath.getInstance(), key, is);
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
			}

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

}
