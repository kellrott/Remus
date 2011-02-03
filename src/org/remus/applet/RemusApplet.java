package org.remus.applet;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.mpstore.MPStore;
import org.remus.CodeFragment;
import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.WorkDescription;


public class RemusApplet {

	public static RemusApplet newApplet( String path, CodeFragment code, int type ) {
		RemusApplet out = new RemusApplet();

		switch (type) {
		case MAPPER: {
			out.workGenerator = MapGenerator.class; //new MapperApplet();	
			break;
		}
		case REDUCER: {
			out.workGenerator = ReduceGenerator.class;	
			break;
		}
		case SPLITTER: {
			out.workGenerator = SplitGenerator.class;	
			break;
		}
		case MERGER: {
			out.workGenerator = MergeGenerator.class;	
			break;
		}
		case PIPE: {
			out.workGenerator = PipeGenerator.class;	
			break;
		}
		}
		if ( out != null ) {
			out.code = code;
			out.path = path;
			out.type = type;
			out.inputs = null;
			try {
				WorkGenerator gen = (WorkGenerator) out.workGenerator.newInstance();
				gen.init(out);
				out.status = new InstanceStatus( out, gen );
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return out;
	}



	public static final int MAPPER = 1;
	public static final int MERGER = 2;
	public static final int SPLITTER = 3;
	public static final int REDUCER = 4;
	public static final int PIPE = 5;

	Class workGenerator;
	String path;
	List<InputReference> inputs = null, lInputs = null, rInputs = null;
	List<String> outputs = null;
	CodeFragment code;
	MPStore datastore;
	int type;
	protected RemusPipeline pipeline = null;
	private InstanceStatus status;

	public void addInput( InputReference in ) {
		if ( inputs == null )
			inputs = new ArrayList<InputReference>();
		inputs.add(in);
	}	

	public void addLeftInput( InputReference in ) {
		if ( lInputs == null )
			lInputs = new LinkedList<InputReference>();
		lInputs.add(in);
		addInput(in);
	}

	public void addRightInput( InputReference in ) {
		if ( rInputs == null )
			rInputs = new LinkedList<InputReference>();
		rInputs.add(in);
		addInput(in);
	}

	public void addOutput( String name ) {
		if ( outputs == null ) 
			outputs = new LinkedList<String>();
		outputs.add(name);
	}

	public CodeFragment getCode() {
		return code;
	}

	public List<InputReference> getInputs() {
		return inputs;
	}

	public String [] getOutputs() {
		if ( outputs != null  )
			return outputs.toArray( new String[0] );
		return new String[0];
	}

	public String getPath() {
		return path;
	}

	public String getSource() {
		return code.getSource();
	}



	public void setPipeline(RemusPipeline remusPipeline) {
		this.pipeline = remusPipeline;		
		this.datastore = remusPipeline.getCodeManager().getApp().getDataStore();
	}

	public RemusPipeline getPipeline() {
		return this.pipeline;
	}


	public boolean isReady( RemusInstance remusInstance ) {
		if ( hasInputs() ) {
			boolean allReady = true;
			for ( InputReference iRef : inputs ) {
				RemusApplet iApplet = getPipeline().getApplet( iRef.getAppletPath() );
				if ( iApplet != null ) {
					if ( !iApplet.isComplete(remusInstance) ) {
						allReady = false;
					}
				} else {
					allReady = false;
				}
			}			
			return allReady;
		}		
		return true;
	}


	public boolean isComplete( RemusInstance remusInstance ) {
		if ( datastore.containsKey(new File("/@done"), remusInstance.toString(), getPath() ) ) {
			return true;
		}
		return false;
	}

	public void setComplete(RemusInstance remusInstance) {
		datastore.add(new File("/@done"), RemusInstance.STATIC_INSTANCE_STR, 0, 0, getPath(), remusInstance.toString() );
	}

	public void finishWork(RemusInstance remusInstance, long jobID) {
		if ( status.hasInstance(remusInstance) ) {
			status.removeWork( remusInstance, jobID );
			datastore.add(new File( getPath() + "@work"), remusInstance.toString(), jobID, 0, getPath(), jobID );
			if ( status.jobCount( remusInstance ) == 0 ) {
				setComplete( remusInstance );
			}
		}
	}

	public boolean hasInputs() {
		if ( inputs == null )
			return false;
		return true;
	}

	public int getType() {
		return type;
	}


	public WorkDescription getWork(RemusInstance inst, int jobID) {
		return status.getWork(inst, jobID);
	}

	public Collection<WorkDescription> getWorkList(RemusInstance job) {
		Collection<WorkDescription> workSet = status.getWorkList(job);
		return workSet;
	}

	public Map getInfo() {
		Map out = new HashMap();
		if ( type==MAPPER ) {
			out.put("mode", "map");
		}
		if ( type==REDUCER ) {
			out.put("mode", "reduce");
		}
		if(type==SPLITTER) {
			out.put("mode", "split");
		}
		if(type==PIPE) {
			out.put("mode", "pipe");
		}
		if(type==MERGER) {
			out.put("mode", "merge");
		}
		if ( outputs != null && outputs.size() > 0 ) {
			out.put("output", outputs);
		}
		return out;
	}

	public void addInstance(RemusInstance instance) {
		status.addInstance(instance);
	}

	public Collection<RemusInstance> getInstanceList() {
		Collection<RemusInstance> out = new HashSet( status.getInstanceList() );
		for ( Object instStr : datastore.get( new File("/@done"), RemusInstance.STATIC_INSTANCE_STR, getPath() ) ) {
			out.add( new RemusInstance( (String)instStr ) );
		}
		return out;
	}


}
