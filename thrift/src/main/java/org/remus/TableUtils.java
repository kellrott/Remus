package org.remus;

import java.util.HashMap;
import java.util.Map;

import org.remus.JSON;
import org.remus.thrift.TableRef;

public class TableUtils {

	public static String RefToJSON(TableRef ref) {
		Map out = new HashMap();
		out.put("instance", ref.instance);
		out.put("table", ref.table);
		return JSON.dumps(out);
	}

	public static String RefToString(TableRef a) {
		return a.instance + ":" + a.table;
	}

	public static TableRef StringToRef(String arName) {
		String [] tmp = arName.split(":");
		if (tmp.length == 2) {
			return new TableRef(tmp[0], tmp[1]);
		}
		return null;
	}

}
