package org.remus.manage;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpstore.MPStore;
import org.remus.RemusApp;
import org.remus.RemusDatabaseException;
import org.remus.RemusInstance;
import org.remus.RemusPath;
import org.remus.RemusPipeline;
import org.remus.work.AppletInstance;
import org.remus.work.RemusApplet;
import org.remus.work.WorkKey;


public class CodeManager {
	RemusApp  parent;
	MPStore datastore;
	public CodeManager( RemusApp parent ) {
		this.parent = parent;
		datastore = parent.getDataStore();
		codeMap = new HashMap<String, RemusApplet>();
	}

	private Map<String,RemusApplet> codeMap;
	public void put( String codePath, RemusApplet code ) {
		codeMap.put(codePath, code);
	}
	public RemusApplet get(String path) {
		return codeMap.get(path);
	}
	public boolean containsKey(String path) {
		return codeMap.containsKey(path);
	}
	public Set<String> keySet() {
		return codeMap.keySet();
	}

	private List<RemusPipeline> pipelines;

	public void mapPipelines() {
		HashMap<String,Integer> colorMap = new HashMap<String,Integer>();
		int i = 0;
		for ( String path : codeMap.keySet() ) {
			colorMap.put(path, i);
			i++;
			for ( String outname : codeMap.get(path).getOutputs() ) {
				colorMap.put(path + "." + outname, i);
				i++;
			}
		}

		boolean change = false;
		do {
			change = false;
			for ( String path : codeMap.keySet() ) {
				if ( codeMap.get(path).hasInputs() ) {
					for ( RemusPath inRef : codeMap.get(path).getInputs() ) {
						if ( colorMap.containsKey( inRef.getPortPath() ) ) {
							int val1 = colorMap.get(path);
							int val2 = colorMap.get(inRef.getPortPath());
							if ( val1 != val2 )  {
								colorMap.put(path, Math.min(val1, val2));
								colorMap.put(inRef.getPortPath(), Math.min(val1, val2));
								change = true;
							}
						}
					}
				}
				for ( String outname : codeMap.get(path).getOutputs() ) {
					if ( colorMap.containsKey( path + "." + outname ) ) {
						int val1 = colorMap.get(path);
						int val2 = colorMap.get(path + "." + outname);
						if ( val1 != val2 )  {
							colorMap.put(path, Math.min(val1, val2));
							colorMap.put(path + "." + outname, Math.min(val1, val2));
							change = true;
						}
					}
				}
			}			
		} while (change);
		Map<Integer,RemusPipeline> out = new HashMap<Integer, RemusPipeline>();
		for ( int color : colorMap.values() ) {
			if ( !out.containsKey(color) ) {
				RemusPipeline pipeline = new RemusPipeline(this);
				for ( String path : codeMap.keySet() ) {
					if ( colorMap.get(path) == color ) {
						pipeline.addApplet( codeMap.get(path) );
					}
				}
				if ( pipeline.appletCount() > 0 )
					out.put(color, pipeline);
			}
		}	
		pipelines = new LinkedList<RemusPipeline>( out.values() );
	}


	public void startWorkQueue() throws RemusDatabaseException {
		for ( RemusPipeline pipeline : pipelines ) {			
			if ( !pipeline.isDynamic() ) {
				RemusInstance pipelineInstance = null;
				for ( RemusApplet applet : pipeline.getMembers() ) {
					for ( RemusInstance inst : applet.getInstanceList() ) {
						if ( pipelineInstance == null ) 
							pipelineInstance = inst;
					}
				}
				if ( pipelineInstance == null ) {
					pipelineInstance = new RemusInstance();
					for ( RemusApplet applet : pipeline.getMembers() ) {
						for ( RemusPath iRef : applet.getInputs() ) {
							if ( iRef.getInputType() == RemusPath.StaticInput ) {
								applet.submit( pipelineInstance, null );
							}
						}
					}
				}				
			}
		}
	}

	public Map<AppletInstance,Set<WorkKey>> getWorkQueue(int maxSize) {		
		Map<AppletInstance,Set<WorkKey>> out = new HashMap<AppletInstance,Set<WorkKey>>();
		for ( RemusPipeline pipeline : pipelines ) {			
			if ( out.size() < maxSize ) {
				out.putAll( pipeline.getWorkQueue( maxSize - out.size() ) );
			}
		}
		return out;		
	}

	public RemusApp getApp() {
		return parent;		
	}
	public int getPipelineCount() {
		return pipelines.size();
	}
	public RemusPipeline getPipeline(int i) {
		return pipelines.get(i);		
	}
	public Collection<RemusPipeline> getPipelines() {
		return pipelines;
	}


}
