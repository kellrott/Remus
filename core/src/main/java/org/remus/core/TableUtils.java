package org.remus.core;

import java.util.HashMap;
import java.util.Map;

import org.remus.JSON;
import org.remus.thrift.AppletRef;

public class TableUtils {

	public static String RefToJSON(AppletRef ref) {
		Map out = new HashMap();
		out.put("pipeline", ref.pipeline);
		out.put("instance", ref.instance);
		out.put("table", ref.applet);
		return JSON.dumps(out);
	}

	public static String RefToString(AppletRef a) {
		return a.pipeline + ":" + a.instance + ":" + a.applet;
	}

	public static AppletRef StringToRef(String arName) {
		String [] tmp = arName.split(":");
		if (tmp.length == 3) {
			return new AppletRef(tmp[0], tmp[1], tmp[2]);
		}
		if (tmp.length == 4) {
			return new AppletRef(tmp[0], tmp[1], tmp[2] + ":" + tmp[3]);
		}
		return null;
	}

}
