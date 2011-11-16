package org.remus.manage;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONAware;
import org.remus.JSON;


public class RemoteJob implements JSONAware {
	private String peerID;
	private String jobID;
	private long workStart;
	private long workEnd;
	private long startTime;
	RemoteJob(String peerID, String jobID, long workStart, long workEnd) {
		this.peerID = peerID;
		this.jobID = jobID;
		this.workStart = workStart;
		this.workEnd = workEnd;
		this.startTime = (new Date()).getTime();
	}

	@Override
	public int hashCode() {
		return peerID.hashCode() + jobID.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RemoteJob) {
			return equals((RemoteJob) obj);
		}
		return super.equals(obj);
	}

	public boolean equals(RemoteJob job) {
		if (job.peerID.equals(peerID) && job.jobID.equals(jobID)) {
			return true;
		}
		return false;
	}

	public String getJobID() {
		return jobID;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public String getPeerID() {
		return peerID;
	}
	
	public long getWorkStart() {
		return workStart;
	}

	public long getWorkEnd() {
		return workEnd;
	}

	@Override
	public String toJSONString() {
		Map out = new HashMap();
		out.put("peer", peerID);
		out.put("jobID", getJobID());
		return JSON.dumps(out);
	}

	
}