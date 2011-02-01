package org.mpstore;

public interface Serializer {
	Object loads(String s);
	String dumps(Object o);
}
