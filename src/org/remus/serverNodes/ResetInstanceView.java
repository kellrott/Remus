package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.mpstore.Serializer;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.work.Submission;

public class ResetInstanceView implements BaseNode {
	RemusPipeline pipeline;

	public ResetInstanceView(RemusPipeline pipeline) {
		this.pipeline = pipeline;
	}

	@Override
	public void doDelete(String name, Map params, String workerID)
	throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void doGet(String name, Map params, String workerID,
			Serializer serial, OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doPut(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doSubmit(String name, String workerID, Serializer serial,
			InputStream is, OutputStream os) throws FileNotFoundException {
		if ( name.length() > 0 ) {
			String subKey = null;
			RemusInstance inst = null;
			Map subMap = null;
			for ( Object subObject : pipeline.getDataStore().get( "/" + pipeline.getID() + "/@submit", RemusInstance.STATIC_INSTANCE_STR, name) ) {
				subMap = (Map) subObject;
				inst = new RemusInstance( (String)subMap.get( Submission.InstanceField ) );
				subKey = name;
			}
			for ( Object instObject : pipeline.getDataStore().get( "/" + pipeline.getID() + "/@instance", RemusInstance.STATIC_INSTANCE_STR, name) ) {
				subKey = (String) instObject;
				inst = new RemusInstance( name );				
				for ( Object subObject : pipeline.getDataStore().get( "/" + pipeline.getID() + "/@submit", RemusInstance.STATIC_INSTANCE_STR, subKey) ) {
					subMap = (Map)subObject;
				}
			}
			if ( inst == null || subKey == null) {
				throw new FileNotFoundException();	
			}			
			subMap.remove( Submission.WorkDoneField );			
			pipeline.deleteInstance(inst);
			pipeline.handleSubmission( subKey , subMap );
			
			try {
				os.write( inst.toString().getBytes() );
				os.write( " Restarted as ".getBytes() );
				os.write( subKey.getBytes() );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
