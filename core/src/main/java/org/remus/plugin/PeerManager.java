package org.remus.plugin;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.remus.RemusRemote;
import org.remus.thrift.BadPeerName;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerAddress;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.PeerType;
import org.remus.thrift.RemusNet;
import org.remus.thrift.RemusNet.Iface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PeerManager {

	private Logger logger;

	private PingThread pingThread;
	private Map<String,PeerInfoThrift> peerMap = new HashMap<String, PeerInfoThrift>();
	private Map<String,Long> lastStat = new HashMap<String, Long>();
	private Set<String> localPeers = new HashSet<String>();

	private Map<String, RemusNet.Iface> ifaceMap = new HashMap<String, Iface>();
	private Map<String,Integer> ifaceAlloc = new HashMap<String, Integer>();

	
	PingThread pThread;
	
	public PeerManager(PluginManager pm, List<PeerAddress> seeds) throws NotImplemented, BadPeerName, TException {
		logger = LoggerFactory.getLogger(PeerManager.class);
		for (PeerAddress pa : seeds) {
			addSeed(pa);
		}
		pThread = new PingThread();
		pThread.start();
	}

	public void stop() {
		pThread.quit();
	}

	class PingThread extends Thread {
		private static final long SLEEPTIME = 30000;
		boolean quit = false;
		Integer waitLock = new Integer(0);

		@Override
		public void run() {
			while (!quit) {
				update();
				synchronized (waitLock) {			
					try {
						waitLock.wait(SLEEPTIME);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

		public void quit() {
			quit = true;
			synchronized (waitLock) {
				waitLock.notifyAll();	
			}
		}
	}

	public void flushOld(long oldest) {
		long minPing = (new Date()).getTime() - oldest;
		List<String> removeList = new LinkedList<String>();
		synchronized (peerMap) {
			synchronized (lastStat) {
				for (String name : lastStat.keySet()) {
					if (lastStat.get(name) < minPing) {
						removeList.add(name);
					}
				}
				for (String name : removeList) {
					peerMap.put(name, null);
				}
			}
		}
	}

	public static String getDefaultAddress() throws UnknownHostException, SocketException {

		return InetAddress.getByName(InetAddress.getLocalHost().getHostName()).getHostAddress();

		/*
		for (Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
			NetworkInterface ifc = ifaces.nextElement();
			if(ifc.isUp()) {
				for( Enumeration<InetAddress> addres = ifc.getInetAddresses(); addres.hasMoreElements(); ) {
					InetAddress addr = addres.nextElement();
					return addr.getHostAddress();
				}
			}
		}
		return null;
		 */
	}


	public void reqPeerInfo(RemusNet.Iface remote) throws NotImplemented, BadPeerName, TException {
		List<PeerInfoThrift> ret = remote.peerInfo(new LinkedList(peerMap.values()));
		for (PeerInfoThrift pi : ret) {
			if (pi.peerType != PeerType.DEAD) {
				logger.info("RECEIVED PEER: " + pi.peerID);
				peerMap.put(pi.peerID, pi);
			}
		}
	}

	public void update() {
		Random r = new Random();
		int tries = 0;
		PeerInfoThrift pi = null;
		if (peerMap.isEmpty()) {
			return;
		}
		do {
			List<PeerInfoThrift> s = new ArrayList<PeerInfoThrift>(peerMap.values());
			pi = s.get(r.nextInt(s.size()));
			if (localPeers.contains(pi.peerID)) {
				pi = null;
			}
			tries++;
		} while (pi == null && tries < 5);
		if (pi != null) {
			try {
				RemusNet.Iface remote = getPeer(pi.peerID);
				if (remote != null) {
					reqPeerInfo(remote);
					returnPeer(remote);
				}
			} catch (TException e) {
				e.printStackTrace();			
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (BadPeerName e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}


	public List<PeerInfoThrift> peerInfo(List<PeerInfoThrift> info)
	throws NotImplemented, BadPeerName, TException {

		Set<String> diffList = new HashSet(peerMap.keySet());
		for (PeerInfoThrift peer : info) {
			if (!peerMap.containsKey(peer.peerID)) {
				if (peer.peerType != PeerType.DEAD) {
					logger.info( "Adding peer: " + peer.peerID );
					peerMap.put(peer.peerID, peer);
				}
			} else {
				if (peer.peerType == PeerType.DEAD) {
					if (peerMap.get(peer.peerID).peerType != PeerType.DEAD) {
						peerMap.get(peer.peerID).peerType = PeerType.DEAD;
					}
				} else {
					if (peerMap.get(peer.peerID).peerType != PeerType.DEAD) {
						diffList.remove(peer.peerID);
					}
				}
			}
		}

		List<PeerInfoThrift> out = new LinkedList<PeerInfoThrift>();
		for (String peerID : diffList ) {
			PeerInfoThrift peer = peerMap.get(peerID);
			if (peer.peerType != PeerType.DEAD) {
				out.add(peer);
			}
		}
		return out;
	}

	public void addSeed(PeerAddress pa) throws NotImplemented, BadPeerName, TException {
		try {
			RemusRemote remote = new RemusRemote(pa.host, pa.port);		
			reqPeerInfo(remote);
			remote.close();
		} catch (TException e) {

		}
	}

	public void addLocalPeer(PluginInterface pi, PeerAddress serverAddr) throws UnknownHostException, SocketException {
		PeerInfoThrift info = pi.getPeerInfo();
		info.setPeerID(UUID.randomUUID().toString());
		info.setAddr(serverAddr);
		peerMap.put(info.getPeerID(), info);
		ifaceMap.put(info.peerID, pi);
		ifaceAlloc.put(info.getPeerID(), 0);
		localPeers.add(info.peerID);
	}



	public PeerInfoThrift getPeerInfo(String peerID) throws NotImplemented, TException {
		Collection<PeerInfoThrift> list = getPeers();
		for (PeerInfoThrift p : list) {
			if (p.peerID.equals(peerID)) {
				return p;
			}
		}	
		return null;
	}

	public RemusNet.Iface getPeer(String peerID) throws TException {
		if (ifaceMap.containsKey(peerID)) {
			ifaceAlloc.put(peerID, ifaceAlloc.get(peerID)+1);
			return ifaceMap.get(peerID);
		}
		PeerInfoThrift p = peerMap.get(peerID);
		if (p != null) {
			try {
				RemusNet.Iface r = new RemusRemote(p.addr.host, p.addr.port);
				ifaceMap.put(p.peerID, r);
				ifaceAlloc.put(peerID, 1);
				return r;
			} catch (TTransportException e) {
				//peer appears to be dead, mark as such
				peerMap.get(p.peerID).peerType = PeerType.DEAD;
			}
		}
		return null;
	}

	public void returnPeer(RemusNet.Iface iface) {
		String peerID = null;
		for ( Entry<String, Iface> e: ifaceMap.entrySet()) {
			if ( iface == e.getValue()) {
				peerID = e.getKey();
			}
		}
		if (peerID != null && !localPeers.contains(peerID)) {
			int c = ifaceAlloc.get(peerID);
			if (c <= 1) {
				logger.debug("RELEASE connection: " + peerID);
				((RemusRemote)iface).close();
				ifaceAlloc.remove(peerID);
				ifaceMap.remove(peerID);
			} else {
				ifaceAlloc.put(peerID, c-1);
			}
		}
	}

	public String getPeerID(RemusNet.Iface plug) {
		for (String key : ifaceMap.keySet()) {
			if (ifaceMap.get(key) == plug) {
				return key;
			}
		}
		return null;
	}

	public boolean isLocalPeer(String peerID) {
		if (localPeers.contains(peerID)) {
			return true;
		}
		return false;
	}

	public Set<String> getWorkers() throws NotImplemented, TException {
		Set<String> out = new HashSet<String>();
		Collection<PeerInfoThrift> list = getPeers();
		for (PeerInfoThrift p : list) {
			if (p.peerType == PeerType.WORKER) {
				out.add(p.peerID);
			}
		}
		return out;
	}

	public String getManager() {
		Set<String> out = new HashSet<String>();
		Collection<PeerInfoThrift> list = getPeers();
		for (PeerInfoThrift p : list) {
			if (p.peerType == PeerType.MANAGER) {
				return p.peerID;
			}
		}
		return null;
	}

	public Set<String> getWorkers(String type) throws NotImplemented, TException {
		Set<String> out = new HashSet<String>();
		Collection<PeerInfoThrift> list = getPeers();
		for (PeerInfoThrift p : list) {
			if (p.peerType == PeerType.WORKER) {
				if (p.workTypes.contains(type)) {
					out.add(p.peerID);
				}
			}
		}
		return out;
	}

	/*
	public int addLocalPeer(String peerID, RemusNet.Iface iface) {
		peerList.put(peerID, iface);
		localPeers.add(peerID);
		if (servers.containsKey(iface)) {
			return servers.get(iface).port;
		}
		return 0;
	}
	 */

	public String getDataServer() {
		Collection<PeerInfoThrift> piList = getPeers();		
		for (PeerInfoThrift pi : piList) {
			if (pi.peerType == PeerType.DB_SERVER) {
				return pi.peerID;
			}
		}
		return null;
	}

	public String getAttachStore() {
		Collection<PeerInfoThrift> piList = getPeers();		
		for (PeerInfoThrift pi : piList) {
			if (pi.peerType == PeerType.ATTACH_SERVER) {
				return pi.peerID;
			}
		}
		return null;
	}


	public Collection<PeerInfoThrift> getPeers() {
		List<PeerInfoThrift> out = new LinkedList<PeerInfoThrift>();
		for (PeerInfoThrift a : peerMap.values()) {
			if (a.peerType != PeerType.DEAD) {
				out.add(a);
			}
		}
		return out;
	}

}
