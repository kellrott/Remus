package org.remus.work;


import java.util.Map;

import org.apache.thrift.TException;
import org.remus.KeyValPair;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.core.AppletInstance;
import org.remus.core.AppletInstanceRecord;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentGenerator implements WorkGenerator {

	@Override
	public void writeWorkTable(AppletInstanceRecord air, RemusDB datastore, RemusAttach attachstore) {

		AppletRef ar = new AppletRef(air.getPipeline(), air.getInstance(), air.getApplet());
		AppletRef arWork = new AppletRef(air.getPipeline(), air.getInstance(), air.getApplet() + Constants.WORK_APPLET);

		int jobID = 0;
		for (String input : air.getSources()) {			
			String key = air.getInstance() + ":" + input;
			try {
				datastore.add(arWork, 0, 0, Integer.toString(jobID), key);
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			jobID++;							
		}
		try {
			long t = datastore.getTimeStamp(ar);
			AppletInstance ai = new AppletInstance(air, datastore, attachstore);
			ai.setWorkStat(0, 0, 0, jobID, t);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void finalizeWork(AppletInstanceRecord air, RemusDB datastore) {
		Logger logger = LoggerFactory.getLogger(AgentGenerator.class);
		AppletRef ar = new AppletRef(air.getPipeline(), air.getInstance(), air.getApplet());
		for (KeyValPair kv : datastore.listKeyPairs(ar)) {
			logger.info("Agent Generating submission: " + kv.getKey());
			AppletRef subAR = new AppletRef(air.getPipeline(), Constants.STATIC_INSTANCE, Constants.SUBMIT_APPLET);
			try {
				if (!datastore.containsKey(subAR, kv.getKey()) ) {
					Map map = (Map) kv.getValue();
					map.remove("_submitKey");
					map.remove("_instance");
					datastore.add(subAR, 0, 0, kv.getKey(), map);
				}
			} catch (NotImplemented e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			}
		}
	}


}
