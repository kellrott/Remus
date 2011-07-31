package org.remus.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.mpstore.MPStore;
import org.remus.RemusInstance;
import org.remus.work.RemusAppletImpl;
import org.remus.work.Submission;

public class DataStackRef {

	RemusInstance instance;
	String viewPath;
	List<String> keys = null;
	MPStore ds;
	
	private DataStackRef() {

	}

	public static DataStackRef fromSubmission( RemusAppletImpl applet, String input, RemusInstance instance) {
		DataStackRef out = new DataStackRef();
		out.instance = instance;
		out.ds = applet.getDataStore();
		if ( applet.getInput().compareTo("?") == 0 ) {
			for ( Object instObj : out.ds.get( applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, instance.toString() ) ) {
				Map inputInfo = (Map)((Map)instObj).get(Submission.InputField);
				String instanceStr = (String) inputInfo.get(Submission.InstanceField);
				String appletStr = (String) inputInfo.get( "_applet" );				
				for ( Object subObj : out.ds.get( "/" + applet.getPipeline().getID() + "/@submit" , RemusInstance.STATIC_INSTANCE_STR, instanceStr) ) {
					instanceStr = (String)((Map)subObj).get(Submission.InstanceField);
				}
				if ( inputInfo.containsKey( Submission.KeysField ) ) {
					out.keys = new ArrayList<String>();
					for ( Object key : (List)inputInfo.get( "_keys" ) ) {
						out.keys.add( (String) key);
					}
				}
				out.instance = new RemusInstance(instanceStr);
				out.viewPath = "/" + applet.getPipeline().getID() + "/" + appletStr;
			}
		} else {
			out.viewPath = "/" + applet.getPipeline().getID() + "/" + input;
		}
		return out;
	}

	public static String pathFromSubmission(RemusAppletImpl applet, String ref,
			RemusInstance instance) {
		return "/" + applet.getPipeline().getID() + "/" + instance.toString() + "/" + ref;
	}

	public long getKeyCount( MPStore ds, int maxCount ) {
		if ( keys != null )
			return keys.size();
		return ds.keyCount( viewPath, instance.toString(), maxCount );			
	}

	public Iterable<String> listKeys( MPStore ds ) {
		if ( keys != null ) {
			return keys;
		}
		return ds.listKeys(viewPath, instance.toString());
	}	

}
