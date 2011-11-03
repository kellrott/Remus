package org.remus.manage;

import org.apache.thrift.TException;
import org.remus.JSON;
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

	public TableWorker(PeerManager peerManager, AppletInstance ai) throws TException, NotImplemented {
		super(peerManager,ai);
		state = WORKING;
	}

	@Override
	public boolean checkWork() throws NotImplemented, TException, InstanceWorkerException {

		if (iface == null && state == WORKING) {

			ai.getReadyJobs(10);
			if ( ai.isComplete())  {
				state = DONE;
			} else {			
				logger.debug("TABLE_MANAGER Contact worker");
				WorkDesc desc = new WorkDesc(ai.getApplet().getType(), 
						WorkMode.findByValue(ai.getApplet().getMode()),
						JSON.dumps(ai.getInstanceInfo()),
						ai.getAppletRef(),
						0L, -1L);

				peerID = borrowPeer();		
				iface = peerManager.getPeer(peerID);
				try {
					jobID = iface.jobRequest(peerManager.getDataServer(), peerManager.getAttachStore(), desc);
				} catch (TException e) {
					e.printStackTrace();
					peerManager.peerFailure(peerID);
					errorPeer(peerID);
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
				errorPeer(peerID);
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

}
