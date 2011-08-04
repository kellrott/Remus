package org.remus.plugin;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusManager;
import org.remus.thrift.PeerType;


public class PluginManager {

	List<PluginInterface> plugins;
	
	public PluginManager(Map<String,Object> params) {
		System.err.println(params);
		plugins = new LinkedList<PluginInterface>();
		
		for (String className : params.keySet()) {
			try {
				Class<PluginInterface> pClass = 
					(Class<PluginInterface>) Class.forName(className);
				PluginInterface plug = (PluginInterface) pClass.newInstance();
				plug.init((Map)params.get(className));		
				plugins.add(plug);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void start() throws Exception {
		for (PluginInterface p : plugins) {
			p.start(this);
		}
	}
	
	static public void main(String [] args) throws Exception {
		
		JSONParser j = new JSONParser();		
		FileReader read = new FileReader(new File(args[0]));
		Object params = j.parse(read);
		
		PluginManager p = new PluginManager((Map) params);
		p.start();
	}

	public RemusDB getDataServer() {
		for (PluginInterface pi : plugins) {
			if (pi.getPeerInfo().peerType == PeerType.DB_SERVER) {
				return (RemusDB) pi;
			}
		}
		return null;
	}
	
	/*
	public RemusDB getServiceByAddress(String address) {
		
	}
	/(
	 * 
	 */
	public RemusAttach getAttachStore() {
		for (PluginInterface pi : plugins) {
			if (pi.getPeerInfo().peerType == PeerType.ATTACH_SERVER) {
				return (RemusAttach) pi;
			}
		}
		return null;
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public RemusManager getManager() {
		for (PluginInterface pi : plugins) {
			if (pi.getPeerInfo().peerType == PeerType.MANAGER) {
				return (RemusManager) pi;
			}
		}
		return null;		
	}
	
}
