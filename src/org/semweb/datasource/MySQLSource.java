package org.semweb.datasource;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.semweb.config.ConfigMap;

public class MySQLSource extends DataSource {

	private Connection conn;

	public MySQLSource() {

	}

	public List<Map<String,Object>> query(String queryStr) {
		List<Map<String,Object>> outList = null;		
		try {
			Statement st = conn.createStatement();
			ResultSet res = st.executeQuery(queryStr);
			outList = new LinkedList<Map<String,Object>>();

			ResultSetMetaData rsmd = res.getMetaData();
			while ( res.next() ) {
				Map<String,Object> outMap = new HashMap<String,Object>();
				for ( int i = 0; i < rsmd.getColumnCount(); i++ ) {
					switch ( rsmd.getColumnType(i+1) ) {
					case java.sql.Types.FLOAT:
						outMap.put(  rsmd.getColumnName(i+1), res.getFloat(i+1) );
						break;
					case java.sql.Types.INTEGER:
						outMap.put(  rsmd.getColumnName(i+1), res.getInt(i+1) );
						break;
					default:
						//System.out.println(  rsmd.getColumnName(i+1) + " " + res.getString(i+1));
						outMap.put(  rsmd.getColumnName(i+1), res.getString(i+1) );
						break;
					}
				}
				outList.add( outMap );
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return outList;
	}

	@Override
	public void init(ConfigMap configMap) throws InitException {
		try {			
			String userName = (String)configMap.get("name");
			String password = (String)configMap.get("password");
			String url = (String)configMap.get("url");
			String driver = (String)configMap.get("driver");

			Class.forName (driver).newInstance ();

			conn = DriverManager.getConnection (url, userName, password);			
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new InitException();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new InitException();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new InitException();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new InitException();
		}
	}

}
