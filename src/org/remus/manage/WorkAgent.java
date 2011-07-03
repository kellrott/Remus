package org.remus.manage;

import org.remus.serverNodes.BaseNode;


public interface WorkAgent extends BaseNode  {

	public void init( WorkManager parent );
	public String getName();
	public void workPoll();
}
