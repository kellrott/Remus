package org.remus.serverNodes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.RemusDB;
import org.remus.core.BaseNode;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.thrift.AppletRef;
import org.remus.work.Submission;

public class PipelineView implements BaseNode {

	HashMap<String,BaseNode> children;

	RemusPipeline pipe;
	public PipelineView(RemusPipeline pipe, RemusDB database) {
		this.pipe = pipe;
		children = new HashMap<String, BaseNode>();
		children.put("@pipeline", new PipelineView(pipe, database) );
		children.put("@submit", new SubmitView(pipe, database) );
		children.put("@status", new PipelineStatusView(pipe, database) );
		children.put("@instance", new PipelineInstanceListViewer(pipe, database) );
		children.put("@agent", new PipelineAgentView(pipe) );

		children.put("@error", new PipelineErrorView(pipe) );

		children.put("@reset", new ResetInstanceView(pipe) );
	}
	
	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		//Deletions should be done through one of the sub-views, or in a parent view
		throw new FileNotFoundException();
	}

	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os)
	throws FileNotFoundException {
		if ( name.length() != 0 ) {
			throw new FileNotFoundException();
		}
		AppletRef arSubmit= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");
		for ( KeyValPair kv : datastore.listKeyPairs(arSubmit) ) {
			Map out = new HashMap();
			out.put(kv.getKey(), kv.getValue() );
			try {
				os.write( JSON.dumps(out).getBytes() );
				os.write("\n".getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void doPut(String name, String workerID, InputStream is, OutputStream os) throws FileNotFoundException {
		System.err.println( "PUTTING:" + name );
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}


	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os) throws FileNotFoundException {
		Map out = new HashMap();
		AppletRef ar = new AppletRef(pipe.getID(), RemusInstance.STATIC_INSTANCE_STR, "/@pipeline");
		try {
			if ( name.length() == 0 ) {
				for ( KeyValPair kv : pipe.getDataStore().listKeyPairs(ar)) {
					out.put(kv.getKey(), kv.getValue() );
				}
			} else {
				for ( Object obj : pipe.getDataStore().get(ar, name )) {
					out.put(name, obj );
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		try {
			os.write( JSON.dumps(out).getBytes() );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void doPut(String name, String workerID, InputStream is, OutputStream os) throws FileNotFoundException {
		if ( pipe != null ) {
			try {
				StringBuilder sb = new StringBuilder();
				byte [] buffer = new byte[1024];
				int len;
				while( (len=is.read(buffer)) > 0 ) {
					sb.append(new String(buffer, 0, len));
				}
				System.err.println( sb.toString() );
				Object data = JSON.loads(sb.toString());
				pipe.getApp().putApplet(pipe, name, data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {
		// TODO Auto-generated method stub

	}

	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}


	class PipelineAttachment implements BaseNode {

		String fileName;
		PipelineAttachment(String fileName) {
			this.fileName = fileName;
		}

		@Override
		public void doDelete(String name, Map params, String workerID) throws FileNotFoundException { }

		@Override
		public void doGet(String name, Map params, String workerID,
				OutputStream os)
		throws FileNotFoundException {
			AppletRef arSubmit= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, null );

			InputStream fis = attachStore.readAttachement(arSubmit, null, fileName);
			if ( fis != null ) {
				byte [] buffer = new byte[1024];
				int len;
				try {
					while ( (len = fis.read(buffer)) >= 0 ) {
						os.write( buffer, 0, len );
					}
					os.close();
					fis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
			} else {
				throw new FileNotFoundException();
			}			
		}

		@Override
		public void doPut(String name, String workerID, InputStream is,
				OutputStream os) throws FileNotFoundException {}

		@Override
		public void doSubmit(String name, String workerID, InputStream is,
				OutputStream os) throws FileNotFoundException {}

		@Override
		public BaseNode getChild(String name) {
			return null;
		}

	}


	@Override
	public BaseNode getChild(String name) {
		if ( children.containsKey(name) )
			return children.get(name);

		AppletRef arSubmit= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@submit");

		try {
			for ( Object subObject : datastore.get(arSubmit, name) ) {
				RemusInstance inst = new RemusInstance( (String)((Map)subObject).get( Submission.InstanceField ) );
				return new PipelineInstanceView(this, inst, datastore);
			}
		} catch (TException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		AppletRef arInstance= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, "/@instance");

		try {
			for ( Object subObject : datastore.get( arInstance, name) ) {
				RemusInstance inst = new RemusInstance( name );
				return new PipelineInstanceView( this, inst  );
			}
		} catch (TException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		AppletRef arBaseAttach= new AppletRef(getID(), RemusInstance.STATIC_INSTANCE_STR, null );

		try { 
			if ( attachStore.hasAttachment( arBaseAttach, null, name ) ) {
				return new PipelineAttachment( name );
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}

	
}
