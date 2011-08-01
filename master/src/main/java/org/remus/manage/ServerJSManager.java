package org.remus.manage;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.remus.BaseNode;
import org.remus.WorkAgent;
import org.remus.WorkManager;
import org.remus.WorkStatus;

public class ServerJSManager implements WorkAgent {

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void doGet(String name, Map params, String workerID,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
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

}
