package org.remus.manage;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.serverNodes.BaseNode;

public class JobTreeManager implements WorkAgent {

	@Override
	public void workPoll() {
		// TODO Auto-generated method stub
		
	}
	
	
	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doDelete(String name, Map params, String workerID)
			throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void init(WorkManager parent) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}



	
}
