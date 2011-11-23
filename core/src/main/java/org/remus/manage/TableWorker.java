package org.remus.manage;

import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusDatabaseException;
import org.remus.core.AppletInstance;
import org.remus.plugin.PeerManager;
import org.remus.thrift.JobState;
import org.remus.thrift.JobStatus;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.WorkMode;

public class TableWorker extends InstanceWorker {

	private String jobID;
	RemusNet.Iface iface;
	String peerID;

	public TableWorker(PeerManager peerManager, WorkerPool wp, AppletInstance ai) throws TException, NotImplemented {
		super(peerManager, wp, ai);
		state = WORKING;
	}

	@Override
	public boolean checkWork() throws NotImplemented, TException, InstanceWorkerException, RemusDatabaseException {

		if (iface == null && state == WORKING) {

			long [] jobs = ai.getReadyJobs(10);
			if ( ai.isComplete() || jobs.length == 0 || ai.isInError())  {
				logger.info("TABLE_MANAGER DONE:" + ai);
				state = DONE;
			} else {			
				logger.debug("TABLE_MANAGER Assign worker: " + ai.getAppletRef());
				WorkDesc desc = new WorkDesc(ai.getRecord().getType(), 
						WorkMode.findByValue(ai.getRecord().getMode()),
						JSON.dumps(ai.getInstanceInfo()),
						ai.getAppletRef(),
						0L, -1L);

				peerID = workPool.borrowWorker(ai.getRecord().getType(), this);
				iface = peerManager.getPeer(peerID);
				try {
					if (iface != null) {
						jobID = iface.jobRequest(peerManager.getDataServer(), peerManager.getAttachStore(), desc);
					}
				} catch (TException e) {
					e.printStackTrace();
					state=DONE;
					peerManager.peerFailure(peerID);
					workPool.errorPeer(peerID);
				}
			}
		} else {
			try {
				JobStatus status = iface.jobStatus(jobID);
				if (status.status == JobState.DONE) {
					state = DONE;
					return true;
				}
				if (status.status == JobState.ERROR) {
					state = ERROR;
					return true;
				}
			} catch (TException e) {
				e.printStackTrace();
				peerManager.peerFailure(peerID);
				workPool.errorPeer(peerID);
				state = DONE;
			}
		}
		return false;
	}


	@Override
	public boolean isDone() {
		if (state == DONE) {
			return true;
		}
		return false;

	}

	@Override
	public void removeJob() {
		// TODO Auto-generated method stub

	}

	@Override
	public String toJSONString() {
		Map out = new HashMap();
		out.put("appletInstance", ai);
		out.put("peer", peerID);
		out.put("jobID", jobID);
		return JSON.dumps(out);
	}

}
