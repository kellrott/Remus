package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.BaseNode;
import org.remus.RemusInstance;
import org.remus.server.RemusPipelineImpl;
import org.remus.work.RemusAppletImpl;
import org.remus.work.Submission;

public class ResetInstanceView implements BaseNode {
	RemusPipelineImpl pipeline;

	public ResetInstanceView(RemusPipelineImpl pipeline) {
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
			OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doPut(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		if ( name.length() > 0 ) {
			String [] tmp = name.split(":");
			if ( tmp.length == 1 ) {

				RemusInstance inst = pipeline.getInstance( name );
				String subKey = pipeline.getSubKey(inst);
				Map subMap = pipeline.getSubmitData(subKey);
				if ( inst == null || subKey == null || subMap == null ) {
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
			} else {
				RemusInstance inst = pipeline.getInstance( tmp[0] );
				RemusAppletImpl applet = pipeline.getApplet(tmp[1]);
				if ( inst != null && applet != null ) {
					try {
						applet.deleteInstance( inst );
						String subKey = pipeline.getSubKey( inst );
						Map params = pipeline.getSubmitData( subKey );
						applet.createInstance( subKey, params, inst);
					} catch (TException e) {
						e.printStackTrace();
						throw new FileNotFoundException();
					}
				}
			}
		}
	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
