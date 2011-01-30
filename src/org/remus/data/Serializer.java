package org.remus.data;

public interface Serializer {
	Object loads(String s);
	String dumps(Object o);
}
