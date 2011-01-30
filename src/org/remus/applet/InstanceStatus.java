package org.remus.applet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.remus.RemusInstance;

public class InstanceStatus {
	public class NodeInstanceStatus {
		public Set<Integer> jobsRemaining;
	}

	public Map<RemusInstance, NodeInstanceStatus> instance;
	RemusApplet parent;
	InstanceStatus(RemusApplet applet) {
		instance = new HashMap<RemusInstance, NodeInstanceStatus>();
		this.parent = applet;
	}

	public void addInstance(RemusInstance remusInstance) {
		NodeInstanceStatus status = new NodeInstanceStatus();
		status.jobsRemaining = new HashSet<Integer>();
		status.jobsRemaining.addAll(parent.getWorkSet(remusInstance));
		instance.put(remusInstance, status);
	}

}
