package org.remus.cassandra;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class NameFlatten {

	public static String encode(String string) {
		String out = null;
		try {
			if ( string == null) {
				out = "%00";
			} else {
				out = URLEncoder.encode(string, "UTF-8" );
				out = out.replaceAll("/", "%2f" );
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return out;
	}
	
	public static File flatten(File workbase, String instance, String applet, String key, String attachment) {
		File out = new File( workbase, encode(instance) );
		out = new File(out, encode(applet) );
		out = new File(out, encode(key) );
		out = new File(out, encode(attachment) );
		return out;
	}
	
}
