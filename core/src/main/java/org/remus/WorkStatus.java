package org.remus;

import java.util.Collection;

public interface WorkStatus {

	RemusApplet getApplet();
	boolean isComplete();
	Collection<Long> getReadyJobs(int i);
	Object getJob(Long jobID);
	Object getInstance();
	void finishJob(long jobID, String workerID);

}
