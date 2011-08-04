package org.remus.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusAttach;
import org.remus.RemusDB;

import org.remus.plugin.PluginManager;
import org.remus.server.RemusDatabaseException;
import org.remus.serverNodes.ManageApp;
import org.remus.serverNodes.ServerStatusView;
import org.remus.serverNodes.StoreInfoView;
import org.remus.thrift.AppletRef;

/**
 * Main Interface to Remus applications. In charge of root database interface and pipeline 
 * management.
 * 
 * @author kellrott
 *
 */

public class RemusApp {

	RemusDB rootStore;
	RemusAttach rootAttachStore;

	public RemusApp( PluginManager plugins ) throws RemusDatabaseException {
		rootStore = plugins.getDataServer();
		rootAttachStore = plugins.getAttachStore();
		//scanSource(srcbase);
		try {
			getPipelines();
		} catch (TException e) {
			throw new RemusDatabaseException( e.toString() );
		}
	}




	public void deletePipeline(RemusPipeline pipe) throws TException {
		for ( String appletName : pipe.getMembers() ) {
			pipe.deleteApplet( pipe.getApplet(appletName) );
		}
		rootStore.deleteValue( new AppletRef( null, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" ), pipe.getID() );
		rootStore.deleteStack( new AppletRef( pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit" ) );
		rootStore.deleteStack( new AppletRef( pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance" ) );		
	}

	/*
	public void putPipeline(String name, Object data) throws TException {
		AppletRef ar = new AppletRef( null, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" );
		rootStore.add(ar, 0L, 0L, name, data );		
		loadPipelines();
	}
	 */

	/*
	public void putApplet(RemusPipelineImpl pipe, String name, Object data) throws TException { 
		AppletRef ar = new AppletRef( pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" );
		rootStore.add( ar, 0L, 0L, name, data );
		loadPipelines();
	}	
	 */

	public RemusPipeline getPipeline( String name ) {
		if ( hasPipeline(name) ) {
			return new RemusPipeline(this, name, rootStore, rootAttachStore);
		} 
		return null;
	}

	public Collection<String> getPipelines() throws TException {
		AppletRef ar = new AppletRef( null, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" );
		return (Collection<String>) rootStore.listKeys(ar);		
	}


	public boolean hasPipeline(String name) {
		AppletRef ar = new AppletRef( null, RemusInstance.STATIC_INSTANCE_STR, "/@pipeline" );		
		try {
			return rootStore.containsKey(ar, name);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}




