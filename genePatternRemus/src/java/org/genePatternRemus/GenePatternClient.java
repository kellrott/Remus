package org.genePatternRemus;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.BaseNode;
import org.remus.WorkAgent;
import org.remus.WorkManager;
import org.remus.WorkStatus;
import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import javax.xml.namespace.QName;



class GenePatternClient implements WorkAgent {

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

	@Override
	public void workPoll() {
		// TODO Auto-generated method stub

	}

	@Override
	public List<String> getWorkTypes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean syncWorkPoll(WorkStatus work) {
		// TODO Auto-generated method stub
		return false;
	}

	public static void main(String [] args) {
		try {
			String endpoint =
				"http://192.168.56.101:8080/gp/services/Analysis?wsdl";

			Service  service = new Service();
			Call     call    = (Call) service.createCall();
			call.setTargetEndpointAddress( new java.net.URL(endpoint) );
			call.setOperationName(new QName("http://192.168.56.101:8080/gp/services/Analysis", "getWebServiceInfo") );
			System.out.println( call );
			String ret = (String) call.invoke( new Object[] { "test" } );

			System.out.println("Sent 'Hello!', got '" + ret + "'");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}