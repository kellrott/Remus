package org.remus.core;

import java.util.List;

import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;

public class DataStackNode implements BaseStackNode {

	private RemusDB db;
	private AppletRef ar;

	public DataStackNode(RemusNet.Iface iface, AppletRef ar) {
		this.db = RemusDB.wrap(iface);
		this.ar = ar;
	}

	@Override
	public void add(String key, String data) {
		try {
			db.add(ar, 0, 0, key, data);
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean containsKey(String key) {
		try {
			return db.containsKey(ar, key);
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public List<String> getValueJSON(String key) {
		try {
			return db.getValueJSON(ar, key);
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<String> keySlice(String keyStart, int count) {
		try {
			return db.keySlice(ar, keyStart, count);
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void delete(String key) {
		// TODO Auto-generated method stub
		
	}



}
