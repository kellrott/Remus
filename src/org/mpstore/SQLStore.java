package org.mpstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
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
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.pool.*;
import org.apache.commons.pool.impl.SoftReferenceObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;



public class SQLStore implements MPStore {

	public class ConnectionFactory extends BasePoolableObjectFactory {

		@Override
		public Object makeObject() throws Exception {
			return DriverManager.getConnection("jdbc:mysql://localhost/test?user=kellrott&dontTrackOpenResources=true&autoReconnect=true" );
		}

		@Override
		public void destroyObject(Object obj) throws Exception {
			((Connection)obj).close();
		}

		@Override
		public boolean validateObject(Object obj) {
			Connection conn = ((Connection)obj);
			try {
				if ( conn.isValid(10000) ) {
					return true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return false;
		}

	}


	private ObjectPool pool = null;
	private Serializer serializer;
	Boolean streaming = true;
	String basePath;
	@Override
	public void init(Serializer serializer, String basePath) {
		this.serializer = serializer;
		this.basePath = basePath;
		try {

			//Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			//connect = DriverManager.getConnection("jdbc:derby:" + basePath + "/derby;create=true" );		

			Class.forName("com.mysql.jdbc.Driver");
			pool = new SoftReferenceObjectPool( new ConnectionFactory() );

			Connection connect = (Connection) pool.borrowObject();			
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
			pool.returnObject(connect);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String getTableName( String path, Boolean create ) {
		try {
			Connection connect = (Connection) pool.borrowObject();			
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
					//, PRIMARY KEY( valkey(900), jobID(4), emitID(4)) 
					st.executeUpdate( "CREATE TABLE " + tableName + " ( valkey VARCHAR(2000), value LONGBLOB, jobID LONG, emitID LONG )" );
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
			pool.returnObject(connect);
			return tableName;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void add(String file, String instance, long jobid, long order, String key, Object data) {
		try {
			Connection connect = (Connection) pool.borrowObject();

			String tableName = getTableName( instance+file, true );
			PreparedStatement st = connect.prepareStatement("INSERT INTO " + tableName +"(jobID, emitID, valkey, value) values(?,?,?,?)");
			st.setLong  ( 1, jobid );
			st.setLong  ( 2, order );
			st.setString( 3, key );
			st.setString( 4, serializer.dumps( data ) );
			st.execute();
			st.close();
			pool.returnObject(connect);

			//connect.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



	@Override
	public void add(String path, String instance, List<KeyValuePair> inputList) {
		try {
			Connection connect = (Connection) pool.borrowObject();

			String tableName = getTableName( instance+path, true );
			PreparedStatement st = connect.prepareStatement("INSERT INTO " + tableName +"(jobID, emitID, valkey, value) values(?,?,?,?)");
			for ( KeyValuePair kv : inputList ) {
				st.setLong  ( 1, kv.getJobID() );
				st.setLong  ( 2, kv.getEmitID() );
				st.setString( 3, kv.getKey() );
				st.setString( 4, serializer.dumps( kv.getValue() ) );
				st.execute();
			}
			st.close();
			pool.returnObject(connect);

			//connect.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean containsKey(String file, String instance, String key) {
		try {
			String tableName = getTableName( instance+file, false );
			if ( tableName != null ) {
				Connection connect = (Connection) pool.borrowObject();
				PreparedStatement st = connect.prepareStatement( "SELECT COUNT(*) FROM " + tableName + " where valkey = ?" );
				st.setString(1, key );	
				ResultSet rs = st.executeQuery();
				rs.next();
				int count = rs.getInt(1);
				rs.close();
				st.close();
				pool.returnObject(connect);

				if ( count > 0 )
					return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
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
				final Connection connect = (Connection) pool.borrowObject();

				final PreparedStatement st = connect.prepareStatement( "SELECT value FROM " + tableName + " where valkey = ?" ,
						ResultSet.TYPE_FORWARD_ONLY,  
						ResultSet.CONCUR_READ_ONLY );
				if ( streaming )
					st.setFetchSize(Integer.MIN_VALUE);
				st.setString(1, key );		

				ResultSet rs = st.executeQuery();			
				return new RowIterator<Object>(rs) {
					@Override
					public Object processRow(ResultSet rs) {
						try {
							String a = rs.getString(1);
							if ( a != null )
								return serializer.loads( a );
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}						
						return null;
					}

					@Override
					public void cleanup() {
						try {
							rs.close();
							st.close();
							pool.returnObject(connect);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}						
					}
				};		
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
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
				final Connection connect = (Connection) pool.borrowObject();
				final PreparedStatement st = connect.prepareStatement( "SELECT distinct(valkey) FROM " + tableName ,
						ResultSet.TYPE_FORWARD_ONLY,  
						ResultSet.CONCUR_READ_ONLY );
				if ( streaming )
					st.setFetchSize(Integer.MIN_VALUE);

				ResultSet rs = st.executeQuery();

				return new RowIterator<String>(rs) {
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

					@Override
					public void cleanup() {
						try {
							rs.close();
							st.close();
							pool.returnObject(connect);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}						
					}
				};		

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
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
				final Connection connect = (Connection) pool.borrowObject();
				final PreparedStatement st = connect.prepareStatement( "SELECT jobID, emitID, valkey, value FROM " + tableName,
						ResultSet.TYPE_FORWARD_ONLY,  
						ResultSet.CONCUR_READ_ONLY );
				if ( streaming )
					st.setFetchSize(Integer.MIN_VALUE);

				ResultSet rs = st.executeQuery();
				final String outFile = file;
				final String outInstance = instance;
				return new RowIterator<KeyValuePair> (rs) {
					@Override
					public KeyValuePair processRow(ResultSet rs) {
						try {
							String keyStr = rs.getString(3);
							String valStr = rs.getString(4);
							Object val = null;
							if ( valStr != null )
								val = serializer.loads(valStr);
							return new KeyValuePair( rs.getLong(1), rs.getLong(2), keyStr, val );
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						return null;
					}

					@Override
					public void cleanup() {
						try {
							rs.close();
							st.close();
							pool.returnObject(connect);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}						
					}
				};
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
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
				final Connection connect = (Connection) pool.borrowObject();

				PreparedStatement st = connect.prepareStatement( "DROP TABLE " + tableName );
				st.execute();
				st.close();
				st = connect.prepareStatement("DELETE FROM mpdata WHERE tableName = ? ");
				st.setString(1, tableName);
				st.execute();
				st.close();
				pool.returnObject(connect);

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
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
				final Connection connect = (Connection) pool.borrowObject();

				PreparedStatement st = connect.prepareStatement( "DELETE FROM " + tableName + " WHERE valkey = ?" );
				st.setString(1, key);
				st.execute();
				st.close();
				pool.returnObject(connect);

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}

	/*
	@Override
	public void delete(String file, String instance, String key, long jobID, long emitID) {
		String tableName = getTableName( instance+file, false );
		if ( tableName != null ) {	
			try {
				final Connection connect = (Connection) pool.borrowObject();
				PreparedStatement st = connect.prepareStatement( "DELETE FROM " + tableName + " WHERE valkey = ? and jobID = ? and emitID = ?" );
				st.setString(1, key);
				st.setLong(2, jobID);
				st.setLong(2, emitID);				
				st.execute();
				st.close();
				pool.returnObject(connect);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		

	}
	 */

	@Override
	public void writeAttachment(String file, String instance, String key, InputStream inputStream) {
		try {
			String tableName = getTableName( instance+file, true );
			if ( tableName != null ) {							
				MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
				sha1.update( file.getBytes() );
				sha1.update( key.getBytes() );
				byte []hash = sha1.digest();
				Formatter format = new Formatter();
				for ( byte b : hash ) {
					format.format("%02x", b);
				}
				String keyDigest = format.toString();
				
				File instanceDir = new File( basePath, instance );
				if ( !instanceDir.exists() ) {
					instanceDir.mkdir();
				}
				File outFile = new File( instanceDir, keyDigest );
				FileOutputStream fos = new FileOutputStream(outFile);
				byte [] buffer = new byte[1024];
				int len;
				while ((len=inputStream.read(buffer))>0) {
					fos.write(buffer, 0, len);
				}
				fos.close();
				
				
				final Connection connect = (Connection) pool.borrowObject();
				PreparedStatement st = connect.prepareStatement( "INSERT INTO " + tableName + "(jobID, emitID, valkey, value) values(0,0,?,?)" );
				st.setString( 1, key );	
				
				st.setString(2, instance + "/" + keyDigest );
				st.execute();
				st.close();		
				pool.returnObject(connect);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchElementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	@Override
	public InputStream readAttachement(String file, String instance, String key) {
		try {
			String tableName = getTableName( instance+file, true );
			if ( tableName != null ) {
				final Connection connect = (Connection) pool.borrowObject();
				PreparedStatement st = connect.prepareStatement( "SELECT value FROM " + tableName + " WHERE valkey = ? " );
				st.setString(1, key);
				ResultSet rs = st.executeQuery();
				rs.next();
				String path = rs.getString(1);
				InputStream is = new FileInputStream( new File(basePath, path) );
				rs.close();
				st.close();
				pool.returnObject(connect);
				return is;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return null;
	}



}
