package org.remus.work;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.serverNodes.BaseNode;

public class InstanceErrorView implements BaseNode {

	RemusApplet applet;
	public InstanceErrorView(RemusApplet applet) {
		this.applet = applet;
	}
	
	@Override
	public void doDelete(Map params) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {
		// TODO Auto-generated method stub

	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
