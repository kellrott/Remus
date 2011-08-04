package org.remus.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import org.apache.thrift.TException;
import org.remus.RemusDB;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;
import org.remus.work.Submission;

public class DataStackRef {

	RemusInstance instance;
	AppletRef viewPath;
	List<String> keys = null;
	RemusDB ds;
	
	private DataStackRef() {

	}

	@SuppressWarnings("unchecked")
	public static DataStackRef fromSubmission( RemusPipeline pipeline, 
			RemusApplet applet, String input, RemusInstance instance) 
	throws TException, NotImplemented, RemusDatabaseException {
		DataStackRef out = new DataStackRef();
		out.instance = instance;
		out.ds = applet.getDataStore();
		
		AppletRef arInstance = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, applet.getID() + "/@instance");
		AppletRef arSubmit   = new AppletRef(pipeline.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");
		
		
		if ( applet.getInput().compareTo("?") == 0 ) {
			for ( Object instObj : out.ds.get( arInstance, instance.toString() ) ) {
				Map inputInfo = (Map)((Map)instObj).get(Submission.InputField);
				if (inputInfo == null) {
					throw new RemusDatabaseException("Submission missing _input field");
				}
				String instanceStr = (String) inputInfo.get(Submission.InstanceField);
				String appletStr = (String) inputInfo.get( "_applet" );				
				for ( Object subObj : out.ds.get( arSubmit, instanceStr) ) {
					instanceStr = (String)((Map)subObj).get(Submission.InstanceField);
				}
				if ( inputInfo.containsKey( Submission.KeysField ) ) {
					out.keys = new ArrayList<String>();
					for ( Object key : (List)inputInfo.get( "_keys" ) ) {
						out.keys.add( (String) key);
					}
				}
				out.instance = new RemusInstance(instanceStr);
				out.viewPath = new AppletRef( pipeline.getID(), out.instance.toString(), appletStr );
			}
		} else {
			out.viewPath = new AppletRef( pipeline.getID(), out.instance.toString(), input );
		}
		return out;
	}

	public static String pathFromSubmission(RemusPipeline pipeline, RemusApplet applet, String ref,
			RemusInstance instance) {
		return "/" + pipeline.getID() + "/" + instance.toString() + "/" + ref;
	}

	public long getKeyCount( RemusDB ds, int maxCount ) throws TException, NotImplemented {
		if ( keys != null )
			return keys.size();
		return ds.keyCount( viewPath, maxCount );			
	}

	public Iterable<String> listKeys( RemusDB ds ) {
		if ( keys != null ) {
			return keys;
		}
		return ds.listKeys(viewPath);
	}	

}
