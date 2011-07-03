package org.mpstore;

import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class JsonSerializer implements Serializer {

	@Override
	public String dumps(Object o) {
		return JSONValue.toJSONString(o);
	}

	
	@Override
	public Object loads(String s) {
		try {
			JSONParser parser=new JSONParser();
			return parser.parse(s);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return null;
	}

	
	
}
