package org.remus;

import java.util.Arrays;
import java.util.Map;

import org.mpstore.MPStore;
import org.remus.work.RemusApplet;
import org.remus.work.Submission;

public class DataStackRef {

	RemusInstance instance;
	String viewPath;
	String key;
	MPStore ds;
	
	private DataStackRef() {

	}

	public static DataStackRef fromSubmission( RemusApplet applet, String input, RemusInstance instance) {
		DataStackRef out = new DataStackRef();
		out.instance = instance;
		out.ds = applet.getDataStore();
		if ( applet.getInput().compareTo("?") == 0 ) {
			for ( Object instObj : out.ds.get( applet.getPath() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, instance.toString() ) ) {
				Map inputInfo = (Map)((Map)instObj).get(Submission.InputField);
				String instanceStr = (String) inputInfo.get(Submission.InstanceField);
				String appletStr = (String) inputInfo.get( "_applet" );				
				for ( Object subObj : out.ds.get( "/" + applet.getPipeline().id + "/@submit" , RemusInstance.STATIC_INSTANCE_STR, instanceStr) ) {
					instanceStr = (String)((Map)subObj).get(Submission.InstanceField);
				}
				out.instance = new RemusInstance(instanceStr);
				out.viewPath = "/" + applet.getPipeline().id + "/" + appletStr;
			}
		} else {
			out.viewPath = "/" + applet.getPipeline().id + "/" + input;
		}
		return out;
	}

	public static String pathFromSubmission(RemusApplet applet, String ref,
			RemusInstance instance) {
		return "/" + applet.getPipeline().id + "/" + instance.toString() + "/" + ref;
	}

	public long getKeyCount( MPStore ds, int maxCount ) {
		if ( key != null )
			return 1;
		return ds.keyCount( viewPath, instance.toString(), maxCount );			
	}

	public Iterable<String> listKeys( MPStore ds ) {
		if ( key != null ) {
			return Arrays.asList(key);
		}
		return ds.listKeys(viewPath, instance.toString());
	}

	

}
