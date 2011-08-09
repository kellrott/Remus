package org.remus;

import java.io.File;
import java.io.FileReader;
import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.remus.plugin.PluginManager;

public class Boot {

	static public void main(String [] args) throws Exception {

		JSONParser j = new JSONParser();		
		FileReader read = new FileReader(new File(args[0]));
		Object params = j.parse(read);

		PluginManager p = new PluginManager((Map) params);
		p.start();
	}
}
