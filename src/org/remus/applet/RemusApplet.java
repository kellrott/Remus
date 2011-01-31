package org.remus.applet;

import java.io.File;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.mpstore.MPStore;
import org.remus.CodeFragment;
import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.WorkDescription;
import org.remus.applet.InstanceStatus.NodeInstanceStatus;


public abstract class RemusApplet {

	public static RemusApplet newApplet( String path, CodeFragment code, int type ) {
		RemusApplet out = null;

		switch (type) {
		case MAPPER: {
			out = new MapperApplet();	
			break;
		}
		case REDUCER: {
			out = new ReducerApplet();	
			break;
		}
		case SPLITTER: {
			out = new SplitterApplet();	
			break;
		}
		case MERGER: {
			out = new MergerApplet();	
			break;
		}
		case PIPE: {
			out = new PipeApplet();	
			break;
		}
		}
		if ( out != null ) {
			out.code = code;
			out.path = path;
			out.type = type;
			out.inputs = new LinkedList<InputReference>();
			out.status = new InstanceStatus(out);
		}
		return out;
	}



	public static final int MAPPER = 1;
	public static final int MERGER = 2;
	public static final int SPLITTER = 3;
	public static final int REDUCER = 4;
	public static final int PIPE = 5;

	String path;
	List<InputReference> inputs = null, lInputs = null, rInputs = null;
	List<String> outputs = null;
	CodeFragment code;
	int type;
	protected RemusPipeline pipeline = null;
	public InstanceStatus status;

	public void addInput( InputReference in ) {
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

	public InputReference [] getInputs() {
		return inputs.toArray( new InputReference[0] );
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

	abstract public Map getDescMap();
	abstract public Collection<Integer> getWorkSet(RemusInstance remusInstance) ;
	abstract public WorkDescription getWork(RemusInstance inst, int jobID);

	public void setPipeline(RemusPipeline remusPipeline) {
		this.pipeline = remusPipeline;		
	}
	
	public RemusPipeline getPipeline() {
		return this.pipeline;
	}

	public void finishWork(RemusInstance remusInstance, long jobID) {
		NodeInstanceStatus is = status.instance.get(remusInstance);
		if ( is != null ) {
			is.jobsRemaining.remove( jobID );
			MPStore ds = pipeline.getCodeManager().getApp().getDataStore();
			ds.add(new File("/@work"), remusInstance.toString(), jobID, getPath(), jobID );
		}
	}

}
