package org.remus.work;

import java.util.List;

import org.remus.RemusInstance;

public class WorkKey {
	public RemusInstance inst;
	public String key;
	public int jobID;
	public String pathStr;
	public List<String> pathArray;
	public WorkKey(RemusInstance inst, int jobID) {
		this.inst = inst;
		this.jobID = jobID;
	}	
	
	@Override
	public int hashCode() {
		return inst.hashCode() + jobID;	
	}
	
	@Override
	public boolean equals(Object obj) {
		WorkKey w = (WorkKey)obj;
		if ( w.inst.equals(inst) && w.jobID == jobID ) {
			return true;
		}
		return false;
	}	
	
}
