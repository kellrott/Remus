package org.mpstore;

import java.io.Serializable;

public interface MPStore {

	@SuppressWarnings("unchecked")
	public void add(Comparable key, Serializable data);

	@SuppressWarnings("unchecked")
	public Iterable<Serializable> get(Comparable key);
	
	@SuppressWarnings("unchecked")
	public Iterable<Comparable> listKeys();
	
}
