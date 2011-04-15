package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.mpstore.Serializer;

public interface BaseNode {
	public BaseNode getChild( String name );	
	public void doGet( String name, Map params, String workerID, Serializer serial, OutputStream os ) throws FileNotFoundException;
	public void doPut( InputStream is, OutputStream os );
	public void doDelete( Map params );
	
}
