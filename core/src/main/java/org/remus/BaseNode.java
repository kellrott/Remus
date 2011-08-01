package org.remus;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public interface BaseNode {
	public BaseNode getChild( String name );	
	public void doGet( String name, Map params, String workerID, OutputStream os ) throws FileNotFoundException;
	public void doPut( String name, String workerID, InputStream is, OutputStream os ) throws FileNotFoundException;
	public void doSubmit( String name, String workerID, InputStream is, OutputStream os ) throws FileNotFoundException;
	public void doDelete( String name, Map params, String workerID ) throws FileNotFoundException;
	
}
