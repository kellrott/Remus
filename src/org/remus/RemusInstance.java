package org.remus;

import java.util.UUID;

public class RemusInstance implements Comparable<RemusInstance>{

	UUID id;
	static final String STATIC_INSTANCE = "00000000-0000-0000-0000-000000000000";
	public RemusInstance() {
		id = UUID.randomUUID();
	}
	

	public RemusInstance(String id) {
		this.id = UUID.fromString(id);
	}
	
	@Override
	public String toString() {
		return id.toString();
	}

	@Override
	public int compareTo(RemusInstance o) {
		return id.compareTo(o.id);
	}
	
	@Override
	public int hashCode() {
		return id.hashCode();
	}

}
