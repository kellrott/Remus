package org.remus.manage;

import java.net.SocketException;
import java.net.UnknownHostException;

public class Util {
	
	public static String getDefaultAddress() throws UnknownHostException, SocketException {

		return "127.0.0.1";

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


}
