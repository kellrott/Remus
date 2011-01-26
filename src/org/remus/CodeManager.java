package org.remus;

import java.util.HashMap;
import java.util.Map;


public class CodeManager {

	RemusApp  parent;
	//PluginManager plugMan;

	public CodeManager( RemusApp parent ) {
		this.parent = parent;
		codeMap = new HashMap<String, CodeFragment>();
		//plugMan = new PluginManager( parent );	
	}

	Map<String,CodeFragment> codeMap;
	public void put( String codePath, CodeFragment code ) {
		codeMap.put(codePath, code);
	}
	
	
}
