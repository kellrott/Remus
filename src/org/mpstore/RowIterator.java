package org.mpstore;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;


public abstract class RowIterator<T> implements Iterable<T>, Iterator<T> {

	ResultSet rs;
	Statement st;
	public RowIterator( ResultSet rs, Statement st ) {
		this.rs = rs;
		this.st = st;
	}
	
	@Override
	public Iterator<T> iterator() {		
		return this;
	}

	@Override
	public boolean hasNext() {
		try {
			boolean more = rs.next();
			if ( !more ) {
				try {
					rs.close();
					st.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return more;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public T next() {
		return processRow( rs );
	}

	public abstract T processRow(ResultSet rs);

	@Override
	public void remove() {
		
	}

	
	
}
