package org.mpstore;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;


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
			connect = DriverManager.getConnection("jdbc:mysql://localhost/test?user=kellrott" );

			Statement st = connect.createStatement();
			try {
				st.executeUpdate( "CREATE TABLE mpdata ( path VARCHAR(1024), instance CHAR(36), jobID LONG, emitID LONG, valkey VARCHAR(1024), value VARCHAR(1024)  )" );
				st.executeUpdate( "CREATE INDEX mpdata_key on mpdata(valkey)" );
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
	public void add(File file, String instance, long jobid, long order, Object key, Object data) {
		try {
			PreparedStatement st = connect.prepareStatement("INSERT INTO mpdata(path, instance, jobID, emitID, valkey, value) values(?,?,?,?,?,?)");
			st.setString( 1, file.getAbsolutePath() );
			st.setString( 2, instance);
			st.setLong  ( 3, jobid );
			st.setLong  ( 4, order );
			st.setString( 5, serializer.dumps( key ) );
			st.setString( 6, serializer.dumps( data ) );		
			st.execute();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean containsKey(File reqFile, String instance, Object key) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT COUNT(*) FROM mpdata where path = ? AND valkey = ? AND instance = ?" );
			st.setString(1, reqFile.getAbsolutePath() );
			st.setString(2, serializer.dumps(key) );		
			st.setString(3, instance);
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
	public Iterable<Object> get(File reqFile, String instance, Object key) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT value FROM mpdata where path = ? AND valkey = ? AND instance = ?" );
			st.setString(1, reqFile.getAbsolutePath() );
			st.setString(2, serializer.dumps(key) );		
			st.setString(3, instance);
			ResultSet rs = st.executeQuery();
			List<Object> out = new LinkedList<Object>();
			while ( rs.next() ) {
				out.add( serializer.loads( rs.getString(1) ) );
			}
			rs.close();
			st.close();
			return out;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	
	@Override
	public KeyValuePair get(File reqFile, String instance, long jobID, long emitID) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT jobID, emitID FROM mpdata where path = ? AND instance = ? AND jobID = ? AND emitID = ?" );
			st.setString(1, reqFile.getAbsolutePath() );
			st.setString(2, instance);
			st.setLong  (3, jobID);
			st.setLong  (4, emitID);
			
			ResultSet rs = st.executeQuery();
			KeyValuePair out = null;
			while ( rs.next() && out == null ) {
				out = new KeyValuePair(this, reqFile.getAbsolutePath(), instance, jobID, emitID);
			}
			rs.close();
			st.close();
			return out;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	
	@Override
	public Object getKey(String path, String instance, long jobID, long emitID) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT valkey FROM mpdata where path = ? AND instance = ? AND jobID = ? AND emitID = ?" );
			st.setString(1, path );
			st.setString(2, instance);
			st.setLong  (3, jobID);
			st.setLong  (4, emitID);
			
			ResultSet rs = st.executeQuery();
			Object out = null;
			while ( rs.next() && out == null ) {
				out = serializer.loads( rs.getString(1) );
			}
			rs.close();
			st.close();
			return out;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;		
	}

	@Override
	public Object getValue(String path, String instance, long jobID, long emitID) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT value FROM mpdata where path = ? AND instance = ? AND jobID = ? AND emitID = ?" );
			st.setString(1, path );
			st.setString(2, instance);
			st.setLong  (3, jobID);
			st.setLong  (4, emitID);
			
			ResultSet rs = st.executeQuery();
			Object out = null;
			while ( rs.next() && out == null ) {
				String serialString = rs.getString(1);
				if ( serialString != null ) {
					out = serializer.loads( serialString );
				}
			}
			rs.close();
			st.close();
			return out;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	
	@Override
	public Iterable<Object> listKeys(File reqFile, String instance) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT distinct(valkey) FROM mpdata where path = ? and instance = ?" );
			st.setString(1, reqFile.getAbsolutePath() );
			st.setString(2, instance);
			ResultSet rs = st.executeQuery();
			List<Object> out = new LinkedList<Object>();
			while ( rs.next() ) {
				out.add( serializer.loads( rs.getString(1) ) );
			}
			rs.close();
			st.close();
			return out;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Iterable<KeyValuePair> listKeyPairs(File reqFile, String instance) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT jobID, emitID FROM mpdata where path = ? and instance = ?" );
			st.setString(1, reqFile.getAbsolutePath() );
			st.setString(2, instance);
			ResultSet rs = st.executeQuery();
			List<KeyValuePair> out = new LinkedList<KeyValuePair>();
			while ( rs.next() ) {
				out.add( new KeyValuePair(this, reqFile.getAbsolutePath(), instance, rs.getLong(1), rs.getLong(2) ));
			}
			rs.close();
			st.close();
			return out;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	
	
}
