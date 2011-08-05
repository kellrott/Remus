package org.remus;

import org.remus.core.BaseStackNode;
import org.remus.mapred.MapReduceCallback;
import org.remus.plugin.PluginInterface;
import org.remus.thrift.WorkMode;

public interface RemusWeb extends PluginInterface {

	RemusAttach getAttachStore();
	RemusDB getDataStore();
	void jsRequest(String string, WorkMode map, 
			BaseStackNode appletView,
			MapReduceCallback mapReduceCallback);

}
