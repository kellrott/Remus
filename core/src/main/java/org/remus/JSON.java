package org.remus;

import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JSON {

	public static String dumps( Object obj ){
		JSONValue j = new JSONValue();
		return JSONValue.toJSONString(obj);
	}
	
	public static Object loads(String str) {
		return JSONValue.parse(str);
	}
	
}
