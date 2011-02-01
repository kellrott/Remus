package org.remus;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mpstore.MPStore;
import org.remus.applet.RemusApplet;


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

	public List<RemusPipeline> pipelines;

	void mapPipelines() {
		HashMap<String,Integer> colorMap = new HashMap<String,Integer>();
		int i = 0;
		for ( String path : codeMap.keySet() ) {
			colorMap.put(path, i);
			i++;
		}

		boolean change;
		do {
			change = false;
			for ( String path : codeMap.keySet() ) {
				if ( codeMap.get(path).hasInputs() ) {
					for ( InputReference inRef : codeMap.get(path).getInputs() ) {
						if ( colorMap.containsKey( inRef.getPath() ) ) {
							int val1 = colorMap.get(path);
							int val2 = colorMap.get(inRef.getPath());
							if ( val1 != val2 )  {
								colorMap.put(path, Math.min(val1, val2));
								colorMap.put(inRef.getPath(), Math.min(val1, val2));
								change = true;
							}
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
				out.put(color, pipeline);
			}
		}	
		pipelines = new LinkedList<RemusPipeline>( out.values() );
	}


	public void startWorkQueue() {
		for ( RemusPipeline pipeline : pipelines ) {			
			if ( !pipeline.dynamic ) {
				if ( pipeline.jobs.size() == 0) {
					RemusInstance instance = new RemusInstance( RemusInstance.STATIC_INSTANCE );
					pipeline.addInstance( instance );
				}
			}
		}
	}

	public List<RemusWork> getWorkQueue(int maxSize) {		
		LinkedList<RemusWork> out = new LinkedList<RemusWork>();

		for ( RemusPipeline pipeline : pipelines ) {			
			if ( out.size() < maxSize ) {
				out.addAll( pipeline.getWorkQueue( maxSize - out.size() ) );
			}
		}
		return out;		
	}

	public RemusApp getApp() {
		return parent;		
	}


}
