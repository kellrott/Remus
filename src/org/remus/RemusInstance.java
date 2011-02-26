package org.remus;

import java.util.UUID;

import org.mpstore.MPStore;

public class RemusInstance implements Comparable<RemusInstance> {

	UUID id;
	public static final String STATIC_INSTANCE_STR = "00000000-0000-0000-0000-000000000000";
	public static final RemusInstance STATIS_INSTANCE = new RemusInstance(STATIC_INSTANCE_STR);
	public RemusInstance() {
		id = UUID.randomUUID();
	}
	
	public RemusInstance(MPStore store, String id) {
		for ( Object orig : store.get("/@alias", RemusInstance.STATIC_INSTANCE_STR, id) ) {
			id = (String)orig;
		}
		this.id = UUID.fromString(id);
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
	
	@Override
	public boolean equals(Object obj) {
		return ((RemusInstance)obj).id.equals(this.id);
	}

}
