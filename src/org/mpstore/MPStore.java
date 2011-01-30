package org.mpstore;

import java.io.File;
import java.io.Serializable;

public interface MPStore {

	public void init(String basePath);
	
	@SuppressWarnings("unchecked")
	public void add(File file, String instance, Comparable key, Serializable data);

	@SuppressWarnings("unchecked")
	public Iterable<Serializable> get(File file, String instance, Comparable key);
	
	@SuppressWarnings("unchecked")
	public Iterable<Comparable> listKeys(File file, String instance);

	public boolean containsKey(File reqFile, String instance, String string);
	
}
