package org.mpstore;

import java.io.File;
import java.io.Serializable;
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

	@Override
	public void init(String basePath) {
		try {
			//Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			//connect = DriverManager.getConnection("jdbc:derby:" + basePath + "/derby;create=true" );		
			
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager.getConnection("jdbc:mysql://localhost/test?user=kellrott" );
			
			Statement st = connect.createStatement();
			try {
				st.executeUpdate( "CREATE TABLE mpdata ( path VARCHAR(1024), instance CHAR(36), valkey VARCHAR(1024), value BLOB  )" );
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
	public void add(File file, Comparable key, Serializable data) {
		try {
			PreparedStatement st = connect.prepareStatement("INSERT INTO mpdata(path, valkey, value) values(?,?,?)");
			st.setString( 1, file.getAbsolutePath() );
			st.setString( 2,  key.toString() );
			st.setString( 3,  data.toString() );		
			st.execute();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean containsKey(File reqFile, String key) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT COUNT(*) FROM mpdata where path = ? AND valkey = ? " );
			st.setString(1, reqFile.getAbsolutePath() );
			st.setString(2, key.toString() );		
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
	public Iterable<Serializable> get(File reqFile, Comparable key) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT value FROM mpdata where path = ? AND valkey = ? " );
			st.setString(1, reqFile.getAbsolutePath() );
			st.setString(2, key.toString() );		
			ResultSet rs = st.executeQuery();
			List<Serializable> out = new LinkedList<Serializable>();
			while ( rs.next() ) {
				out.add( rs.getString(1) );
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
	public Iterable<Comparable> listKeys(File reqFile) {
		try {
			PreparedStatement st = connect.prepareStatement( "SELECT distinct(valkey) FROM mpdata where path = ? " );
			st.setString(1, reqFile.getAbsolutePath() );
			ResultSet rs = st.executeQuery();
			List<Comparable> out = new LinkedList<Comparable>();
			while ( rs.next() ) {
				out.add( rs.getString(1) );
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
