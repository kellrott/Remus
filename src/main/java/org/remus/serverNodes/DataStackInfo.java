package org.remus.serverNodes;

import java.util.HashMap;
import java.util.Map;

import org.remus.RemusPipeline;
import org.remus.server.RemusPipelineImpl;

public class DataStackInfo {

	public static final String PARAM_FLAG = "info";
	
	public static Object formatInfo( Class src, String type, RemusPipeline pipeline ) {
		Map out = new HashMap();		
		out.put("_type", type );
		out.put("_class", src.toString() );
		out.put("_pipeline", pipeline.getID() );
		return out;
	}

}
