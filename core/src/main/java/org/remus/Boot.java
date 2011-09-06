package org.remus;

import java.io.File;
import java.io.FileReader;
import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.remus.plugin.PluginManager;
import org.yaml.snakeyaml.Yaml;

public class Boot {

	static public void main(String [] args) throws Exception {
		Object params = null;
		if (args[0].endsWith(".json")) {
			JSONParser j = new JSONParser();		
			FileReader read = new FileReader(new File(args[0]));
			params = j.parse(read);
		} else if ( args[0].endsWith(".yaml")) {
			Yaml y = new Yaml();
			FileReader read = new FileReader(new File(args[0]));
			params = y.load(read);
		}
		PluginManager p = new PluginManager((Map) params);
		p.start();
	}
}
