package org.mpstore;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;



public abstract class RowIterator<T> implements Iterable<T>, Iterator<T> {

	ResultSet rs;
	
	public RowIterator( ResultSet rs ) {
		this.rs = rs;
		
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
				cleanup();
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
	public abstract void cleanup();
	
	@Override
	public void remove() {

	}
	
}
