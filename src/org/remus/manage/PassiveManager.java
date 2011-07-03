package org.remus.manage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpstore.Serializer;
import org.remus.serverNodes.BaseNode;


public class PassiveManager implements WorkAgent {

	private WorkStatus activeStack = null;
	private Set<Long> activeSet;
	private Set<Long> assignSet;

	private Map<String,Set<Long>> workerSets;
	private Map<String,Date> lastAccess;
	private WorkManager parent;
	private int assignRate;
	private HashMap<String, Date> finishTimes;

	public static final int MAX_REFRESH_TIME = 30 * 1000;



	@Override
	public void init(WorkManager parent) {
		this.parent = parent;		
		workerSets = new HashMap<String, Set<Long>>();
		activeSet = new HashSet<Long>();
		assignSet = new HashSet<Long>();
		assignRate = 1;
		lastAccess = new HashMap<String, Date>();
		finishTimes = new HashMap<String,Date>();
	}

	@Override
	public String getName() {
		return "passive";
	}

	@Override
	public void workPoll() {
		//Passive work manager doesn't do active work,
		//so it doesn't start anything when workPoll'ed
	}

	/************* WEB INTERFACE ****************/

	@Override
	public BaseNode getChild(String name) {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	void fillWorkerSet(String workerID) {
		do {
			if ( activeStack == null ) {
				activeStack = parent.requestWorkStack(this);
				if ( activeStack == null )
					return;
			}
			if ( activeStack.isComplete() ) {
				parent.completeWorkStack(activeStack);
				activeStack = null;
			}
		} while ( activeStack == null );

		if ( activeStack != null ) {
			Set<Long> workerMap = workerSets.get( workerID );
			if ( workerMap == null ) {
				workerMap = new HashSet<Long>();
				workerSets.put(workerID, workerMap);
			}
			if ( activeSet.size() + assignSet.size() < assignRate ) {
				Collection<Long> workSet = activeStack.getReadyJobs( workerSets.size() * assignRate );
				activeSet.addAll( workSet );
				activeSet.removeAll( assignSet );
			}
			while ( workerMap.size() < assignRate && activeSet.size() > 0) {
				long id = activeSet.iterator().next();
				workerMap.add( id );
				activeSet.remove( id );
				assignSet.add( id );
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void doGet(String name, Map params, String workerID, Serializer serial,
			OutputStream os) throws FileNotFoundException {

		if ( params.containsKey("request") ) {
			if ( workerID != null ) {
				fillWorkerSet( workerID );
			} else {
				throw new FileNotFoundException();
			}
		}

		try {
			Map out = new HashMap();
			for ( String worker : workerSets.keySet() ) {
				Set<Long> set = workerSets.get(worker);
				if ( set.size() > 0 && activeStack != null ) {
					Map kMap = new HashMap();
					for ( Long jobID : set ) {
						Object value = activeStack.getJob( jobID );
						kMap.put( jobID, value);
					}
					Map aMap = new HashMap();
					aMap.put(  activeStack.getApplet().getID(), kMap);
					Map iMap = new HashMap();
					iMap.put(activeStack.getInstance(), aMap);
					Map pMap = new HashMap();
					pMap.put(activeStack.getApplet().getPipeline().getID(), iMap);
					out.put(worker, pMap);
				} else {
					out.put(worker, new HashMap() );
				}
			}
			os.write( serial.dumps( out ).getBytes() );

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void doPut(String name, String workerID, Serializer serial, InputStream is, OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		try {

			BufferedReader br = new BufferedReader( new InputStreamReader(is) );
			String curline = null;
			while ((curline=br.readLine())!= null ) {
				Map m = (Map)serial.loads( curline );
				for ( Object instObj : m.keySet() ) {
					for ( Object appletObj : ((Map)m.get(instObj)).keySet() ) {
						List jobList = (List)((Map)m.get(instObj)).get(appletObj);
						for ( Object key2 : jobList ) {
							long jobID = Long.parseLong( key2.toString() );
							//TODO:add emit id count check
							activeStack.finishJob( jobID, workerID );
							synchronized ( finishTimes ) {			
								Date d = new Date();		
								Date last = finishTimes.get( workerID );
								if ( last != null ) {
									if ( d.getTime() - last.getTime() > MAX_REFRESH_TIME ) {
										assignRate /= 2;
									}
									assignRate++;
								}
								finishTimes.put(workerID, d);
							}

						}

					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
