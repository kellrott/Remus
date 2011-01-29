package org.remus;

import java.util.UUID;

public class RemusInstance implements Comparable<RemusInstance>{

	UUID id;
	static final UUID STATIC_INSTANCE = UUID.fromString("00000000-0000-0000-0000-000000000000");
	public RemusInstance() {
		id = UUID.randomUUID();
	}
	

	public RemusInstance(UUID id) {
		this.id = id;
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
