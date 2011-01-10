package org.semweb.plugins;

import java.io.Serializable;

import org.json.JSONException;
import org.json.JSONObject;
import org.semweb.config.PluginConfig;
import org.semweb.pluginterface.WriterInterface;

public class Json implements WriterInterface {

	String curSource;
	@Override
	public void prepWriter(String config) {
		curSource = config;
	}

	@Override
	public Object write(Serializable val) {
		try {
			JSONObject json = new JSONObject( curSource );
			return json;
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void init(PluginConfig config) {
		// TODO Auto-generated method stub
		
	}

}
