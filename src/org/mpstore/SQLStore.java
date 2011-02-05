package org.mpstore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class SQLStore implements MPStore {
	private Connection connect = null;
	private Serializer serializer;
	@Override
	public void init(Serializer serializer, String basePath) {
		this.serializer = serializer;
		try {
			//Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			//connect = DriverManager.getConnection("jdbc:derby:" + basePath + "/derby;create=true" );		

			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection("jdbc:mysql://localhost/test?user=kellrott&dontTrackOpenResources=true" );
			//connect.setAutoCommit(false);
			Statement st = connect.createStatement();
			try {
				//st.executeUpdate( "CREATE TABLE mpdata ( path VARCHAR(1024), instance CHAR(36), jobID LONG, emitID LONG, valkey VARCHAR(1024), value LONGBLOB  )" );
				st.executeUpdate( "CREATE TABLE mpdata ( path VARCHAR(1024), jobID LONG, emitID LONG, valkey VARCHAR(1024), value LONGBLOB  )" );
				st.executeUpdate( "CREATE INDEX mpdata_key on mpdata(valkey)" );
				st.executeUpdate( "CREATE INDEX mpdata_path on mpdata(path)" );
			} catch (SQLException e) {
				e.printStackTrace();
				//if these fail, it's because the tables where already setup
				//derby doesn't have 'IF NOT EXISTS'
			}
			st.close();

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void add(String file, String instance, long jobid, long order, Object key, Object data) {
		try {
			PreparedStatement st = connect.prepareStatement("INSERT INTO mpdata(path, jobID, emitID, valkey, value) values(?,?,?,?,?)");
			st.setString( 1, instance + file );
			st.setLong  ( 2, jobid );
			st.setLong  ( 3, order );
			st.setString( 4, serializer.dumps( key ) );
			st.setString( 5, serializer.dumps( data ) );
			/*
			PreparedStatement st = connect.prepareStatement("INSERT INTO mpdata(path, instance, jobID, emitID, valkey, value) values(?,?,?,?,?,?)");
			st.setString( 1, file );
			st.setString( 2, instance);
			st.setLong  ( 3, jobid );
			st.setLong  ( 4, order );
			st.setString( 5, serializer.dumps( key ) );
			st.setString( 6, serializer.dumps( data ) );
			 */
			st.execute();
			st.close();
			//connect.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean containsKey(String reqFile, String instance, Object key) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT COUNT(*) FROM mpdata where path = ? AND valkey = ?" );
			st.setString(1, instance + reqFile );
			st.setString(2, serializer.dumps(key) );		
			/*
			PreparedStatement st = connect.prepareStatement( "SELECT COUNT(*) FROM mpdata where path = ? AND valkey = ? AND instance = ?" );
			st.setString(1, reqFile );
			st.setString(2, serializer.dumps(key) );		
			st.setString(3, instance);
			 */
			ResultSet rs = st.executeQuery();
			rs.next();
			int count = rs.getInt(1);
			rs.close();
			st.close();
			if ( count > 0 )
				return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Iterable<Object> get(String reqFile, String instance, Object key) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT value FROM mpdata where path = ? AND valkey = ?" ,
					ResultSet.TYPE_FORWARD_ONLY,  
                    ResultSet.CONCUR_READ_ONLY );
			st.setFetchSize(Integer.MIN_VALUE);
			st.setString(1, instance  + reqFile );
			st.setString(2, serializer.dumps(key) );		

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

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}


	@Override
	public Iterable<Object> listKeys(String reqFile, String instance) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT distinct(valkey) FROM mpdata where path = ? ",
					ResultSet.TYPE_FORWARD_ONLY,  
                    ResultSet.CONCUR_READ_ONLY );
			st.setFetchSize(Integer.MIN_VALUE);
			st.setString(1, instance + reqFile );

			ResultSet rs = st.executeQuery();

			return new RowIterator<Object>(rs,st) {
				@Override
				public Object processRow(ResultSet rs) {
					try {
						return serializer.loads( rs.getString(1) );
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return null;
				}				
			};		

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Iterable<KeyValuePair> listKeyPairs(String reqFile, String instance) {
		try {
			/*
			PreparedStatement st = connect.prepareStatement( "SELECT jobID, emitID, valkey, value FROM mpdata where path = ? and instance = ?" );
			st.setString(1, reqFile );
			st.setString(2, instance);
			 */
			PreparedStatement st = connect.prepareStatement( "SELECT jobID, emitID, valkey, value FROM mpdata where path = ? ",
					ResultSet.TYPE_FORWARD_ONLY,  
                    ResultSet.CONCUR_READ_ONLY );
			st.setFetchSize(Integer.MIN_VALUE);
			st.setString(1, instance + reqFile );

			ResultSet rs = st.executeQuery();
			final String outFile = reqFile;
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
							key = serializer.loads(keyStr);
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
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public void close() {
	
	}
}
