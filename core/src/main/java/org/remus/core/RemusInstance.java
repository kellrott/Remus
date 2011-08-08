package org.remus.core;

import java.util.UUID;

import java.util.Map;
import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.work.Submission;

//import org.mpstore.MPStore;

public class RemusInstance implements Comparable<RemusInstance> {

	UUID id;
	public static final String STATIC_INSTANCE_STR = "00000000-0000-0000-0000-000000000000";
	public static final RemusInstance STATIC_INSTANCE = new RemusInstance(STATIC_INSTANCE_STR);

	public RemusInstance() {
		id = UUID.randomUUID();
	}


	static public RemusInstance getInstance(RemusDB store, String pipeline, String id) throws TException, NotImplemented {
		RemusInstance out = null;
		AppletRef arSubmit = new AppletRef(pipeline, STATIC_INSTANCE_STR, "/@submit");
		AppletRef arInst = new AppletRef(pipeline, STATIC_INSTANCE_STR, "/@instance");

		for (Object subObj : store.get(arSubmit, id)) {
			if (((Map) subObj).containsKey(Submission.InstanceField)) {
				out = new RemusInstance((String) ((Map) subObj).get(Submission.InstanceField));
			}
		}
		for (Object instObj : store.get(arInst, id)) {
			out = new RemusInstance(id);			
		}
		return out;
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
