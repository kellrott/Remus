package org.remus.langs;

import java.io.Serializable;

import org.json.JSONException;
import org.json.JSONObject;
import org.remus.PluginConfig;
import org.remus.mapred.PipeInterface;

public class Json implements PipeInterface {

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
