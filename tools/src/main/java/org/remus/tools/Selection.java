package org.remus.tools;

import java.util.Map;

import org.remus.JSON;

public class Selection {
	public static final int ALL = 1;
	public static final int KEY = 2;
	public static final int FIELD = 3;
	
	public String fieldName;
	public int fieldType;

	public Selection(String name) {
		this.fieldName = name;
		fieldType = FIELD;
	}

	public Selection(int type) {
		this.fieldType = type;
	}

	public String getString(String key, Object val) {
		switch (fieldType) {
		case ALL: {
			return key + "|" + JSON.dumps(val);			
		}
		case KEY: {
			return key;			
		}
		case FIELD:  {
			return ((Map) val).get(fieldName).toString();
		}		
		}
		return "";
	}
}
