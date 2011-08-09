package org.remus.plugin;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusManager;
import org.remus.RemusWorker;
import org.remus.thrift.PeerType;


public class PluginManager {

	List<PluginInterface> plugins;

	public PluginManager(Map<String,Object> params) {
		plugins = new LinkedList<PluginInterface>();

		for (String className : params.keySet()) {
			try {
				Map pMap = (Map)params.get(className);
				Class<PluginInterface> pClass = 
					(Class<PluginInterface>) Class.forName(className);
				PluginInterface plug = (PluginInterface) pClass.newInstance();
				Map config = null;
				if (pMap.containsKey("config")) {
					config = (Map)pMap.get("config");
				}				
				plug.init(config);
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
		for (PluginInterface pi : plugins ) {
			pi.stop();
		}

	}

	public RemusManager getManager() {
		for (PluginInterface pi : plugins) {
			if (pi.getPeerInfo().peerType == PeerType.MANAGER) {
				return (RemusManager) pi;
			}
		}
		return null;		
	}

	public Set<RemusWorker> getWorkers() {
		Set<RemusWorker> out = new HashSet<RemusWorker>();
		for ( PluginInterface pi : plugins ) {
			if ( pi.getPeerInfo().peerType == PeerType.WORKER ) {
				out.add((RemusWorker)pi);
			}
		}
		return out;
	}

}
