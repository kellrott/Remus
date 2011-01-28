package org.remus;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class CodeManager {

	RemusApp  parent;
	//PluginManager plugMan;

	public CodeManager( RemusApp parent ) {
		this.parent = parent;
		codeMap = new HashMap<String, RemusApplet>();
		//plugMan = new PluginManager( parent );	
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
	
	
}
