package org.mpstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Formatter;

import javax.servlet.ServletInputStream;


public class SQLStore implements MPStore {
	private Connection connect = null;
	private Serializer serializer;
	Boolean streaming = false;
	String basePath;
	@Override
	public void init(Serializer serializer, String basePath) {
		this.serializer = serializer;
		this.basePath = basePath;
		try {
			//Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			//connect = DriverManager.getConnection("jdbc:derby:" + basePath + "/derby;create=true" );		

			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection("jdbc:mysql://localhost/test?user=kellrott&dontTrackOpenResources=true" );

			ResultSet rs = connect.getMetaData().getTables(null, null, "mpdata", null);
			boolean found = false;
			if ( rs.next() ) {
				found = true;
			}
			rs.close();

			if ( !found  ) {
				Statement st = connect.createStatement();
				try {
					st.executeUpdate( "CREATE TABLE mpdata ( path VARCHAR(2048), tablename VARCHAR(2048) )" );
					st.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String getTableName( String path, Boolean create ) {
		try {
			PreparedStatement st = connect.prepareStatement("SELECT tablename FROM mpdata WHERE path = ?");
			st.setString(1, path);
			ResultSet rs = st.executeQuery();
			String tableName = null;
			boolean entryFound = false;
			if ( rs.next() ) {
				entryFound = true;
				tableName = rs.getString(1);
			}

			boolean tableFound = false;			
			if ( tableName != null ) {
				ResultSet rs2 = connect.getMetaData().getTables(null, null, tableName, null);
				if ( rs2.next() ) {
					tableFound = true;
				}
				rs2.close();
			}

			if ( create && (!entryFound || !tableFound) ) {
				MessageDigest md = MessageDigest.getInstance("SHA-1");
				md.update( path.getBytes() );
				Formatter format = new Formatter();
				for ( byte b : md.digest() ) {
					format.format("%02x", b);
				}
				String digest = format.toString();
				tableName = "mpdata_" + digest;
				if ( !tableFound ) {
					st.executeUpdate( "CREATE TABLE " + tableName + " ( valkey VARCHAR(1024), value LONGBLOB, jobID LONG, emitID LONG  )" );
					st.executeUpdate( "CREATE INDEX " + tableName + "_index on "+ tableName + "(valkey)" );				
				}
				if ( !entryFound ) {
					PreparedStatement st2 = connect.prepareStatement( "INSERT INTO mpdata(path,tablename) VALUES( ?, ? ) " );
					st2.setString(1, path );
					st2.setString(2, tableName );				
					st2.execute();
					st2.close();
				}
			}			
			return tableName;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void add(String file, String instance, long jobid, long order, String key, Object data) {
		try {
			String tableName = getTableName( instance+file, true );
			PreparedStatement st = connect.prepareStatement("INSERT INTO " + tableName +"(jobID, emitID, valkey, value) values(?,?,?,?)");
			st.setLong  ( 1, jobid );
			st.setLong  ( 2, order );
			st.setString( 3, key );
			st.setString( 4, serializer.dumps( data ) );
			st.execute();
			st.close();
			//connect.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean containsKey(String file, String instance, String key) {
		try {
			String tableName = getTableName( instance+file, false );
			if ( tableName != null ) {
				PreparedStatement st = connect.prepareStatement( "SELECT COUNT(*) FROM " + tableName + " where valkey = ?" );
				st.setString(1, key );	
				ResultSet rs = st.executeQuery();
				rs.next();
				int count = rs.getInt(1);
				rs.close();
				st.close();
				if ( count > 0 )
					return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Iterable<Object> get(String file, String instance, String key) {
		try {
			String tableName = getTableName( instance+file, false );
			if ( tableName != null ) {
				PreparedStatement st = connect.prepareStatement( "SELECT value FROM " + tableName + " where valkey = ?" ,
						ResultSet.TYPE_FORWARD_ONLY,  
						ResultSet.CONCUR_READ_ONLY );
				if ( streaming )
					st.setFetchSize(Integer.MIN_VALUE);
				st.setString(1, key );		

				ResultSet rs = st.executeQuery();			
				return new RowIterator<Object>(rs,st) {
					@Override
					public Object processRow(ResultSet rs) {
						try {
							String a = rs.getString(1);
							return serializer.loads( a );
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						return null;
					}
				};		
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new ArrayList<Object>();
	}


	@Override
	public Iterable<String> listKeys(String file, String instance) {
		try {
			String tableName = getTableName( instance+file, false );
			if ( tableName != null ) {
				PreparedStatement st = connect.prepareStatement( "SELECT distinct(valkey) FROM " + tableName ,
						ResultSet.TYPE_FORWARD_ONLY,  
						ResultSet.CONCUR_READ_ONLY );
				if ( streaming )
					st.setFetchSize(Integer.MIN_VALUE);

				ResultSet rs = st.executeQuery();

				return new RowIterator<String>(rs,st) {
					@Override
					public String processRow(ResultSet rs) {
						try {
							return rs.getString(1);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						return null;
					}				
				};		
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new ArrayList<String>();
	}

	@Override
	public Iterable<KeyValuePair> listKeyPairs(String file, String instance) {
		try {
			String tableName = getTableName( instance+file, false );
			if ( tableName != null ) {
				PreparedStatement st = connect.prepareStatement( "SELECT jobID, emitID, valkey, value FROM " + tableName,
						ResultSet.TYPE_FORWARD_ONLY,  
						ResultSet.CONCUR_READ_ONLY );
				if ( streaming )
					st.setFetchSize(Integer.MIN_VALUE);

				ResultSet rs = st.executeQuery();
				final String outFile = file;
				final String outInstance = instance;
				return new RowIterator<KeyValuePair> (rs,st) {
					@Override
					public KeyValuePair processRow(ResultSet rs) {
						try {
							String keyStr = rs.getString(3);
							String valStr = rs.getString(4);
							Object key = null;
							Object val = null;
							if ( keyStr != null )
								key = keyStr;
							if ( valStr != null )
								val = serializer.loads(valStr);
							return new KeyValuePair(outFile, outInstance, rs.getLong(1), rs.getLong(2), key, val );
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						return null;
					}
				};
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new ArrayList<KeyValuePair>();
	}

	@Override
	public void close() {

	}

	@Override
	public void delete(String file, String instance) {
		String tableName = getTableName( instance+file, false );
		if ( tableName != null ) {	
			try {
				PreparedStatement st = connect.prepareStatement( "DROP TABLE " + tableName );
				st.execute();
				st.close();
				st = connect.prepareStatement("DELETE FROM mpdata WHERE tableName = ? ");
				st.setString(1, tableName);
				st.execute();
				st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void delete(String file, String instance, String key) {
		String tableName = getTableName( instance+file, false );
		if ( tableName != null ) {	
			try {
				PreparedStatement st = connect.prepareStatement( "DELETE FROM " + tableName + " WHERE valkey = ?" );
				st.setString(1, key);
				st.execute();
				st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}

	@Override
	public void writeAttachment(String file, String instance, String key, ServletInputStream inputStream) {
		File appletDir = new File( basePath, file );
		File instanceDir = new File( appletDir, instance );
		if ( !instanceDir.exists() ) {
			instanceDir.mkdirs();
		}
		File outFile = new File(instanceDir, key);
		try {
			FileOutputStream fos = new FileOutputStream(outFile);
			byte [] buffer = new byte[1024];
			int len;
			while ((len=inputStream.read(buffer))>0) {
				fos.write(buffer, 0, len);
			}
			fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}


}
