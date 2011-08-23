package org.remus.manage;

import java.util.List;

import org.remus.RemusDatabaseException;
import org.remus.core.BaseStackNode;
import org.remus.core.RemusApp;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PluginManager;
import org.remus.thrift.AppletRef;

public class AppletInstanceStack implements BaseStackNode {

	private RemusApp app;

	public AppletInstanceStack(PluginManager plugins) {
		try {
			app = new RemusApp(plugins.getPeer(plugins.getDataServer()), null);
		} catch (RemusDatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void add(AppletRef stack, long jobID, long emitID, String key, String data) {
		
	}

	@Override
	public boolean containsKey(AppletRef stack, String key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<String> getValueJSON(AppletRef stack, String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> keySlice(AppletRef stack, String keyStart, int count) {
		RemusPipeline pipe = app.getPipeline(stack.pipeline);
		// TODO Auto-generated method stub
		return null;
	}
	
}
