package org.remus;


public class WorkDescription {
	public Object desc=null;
	public WorkReference ref;
	public WorkDescription(WorkReference ref, Object desc) {
		this.desc = desc;
		this.ref = ref;
	}
	
	Object getDesc() {
		return desc;
	}
	
}
