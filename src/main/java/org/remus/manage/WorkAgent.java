package org.remus.manage;

import java.util.List;

import org.remus.serverNodes.BaseNode;


public interface WorkAgent extends BaseNode  {

	/**
	 * Initialize Work Agent
	 * @param parent The WorkManager the Agent should interface with in 
	 * order procure work info
	 */
	public void init( WorkManager parent );
	
	/**
	 * Return unique name of Work Agent
	 * @return Unique name of WorkAgent
	 */
	public String getName();
	
	/**
	 * workPoll is called when server has updated work lists, and wants
	 * the workAgents to check the queues for work they can handle
	 */
	public void workPoll();
	
	/**
	 * Return a list of work types the workAgent is able to except
	 * @return
	 */
	public List<String> getWorkTypes();
	
	/**
	 * An advertisment for work that must be complete before the function
	 * returns. Return 'false' if unable to comply, true if the work as 
	 * been done 
	 * @param work
	 * @return Work completion status
	 */
	public boolean syncWorkPoll(WorkStatus work);
}
