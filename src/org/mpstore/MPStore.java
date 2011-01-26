package org.mpstore;

import java.io.File;
import java.io.Serializable;

public interface MPStore {

	public void init(String basePath);
	
	@SuppressWarnings("unchecked")
	public void add(File file, Comparable key, Serializable data);

	@SuppressWarnings("unchecked")
	public Iterable<Serializable> get(File file, Comparable key);
	
	@SuppressWarnings("unchecked")
	public Iterable<Comparable> listKeys(File file);

	public boolean containsKey(File reqFile, String string);
	
}
