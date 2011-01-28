package org.remus;

import java.util.LinkedList;
import java.util.List;


public class RemusApplet {
	String path;
	List<InputReference> inputs = null, lInputs = null, rInputs = null;
	List<String> outputs = null;
	CodeFragment code;
	
	public RemusApplet( String path, CodeFragment code ) {
		this.code = code;
		this.path = path;
		inputs = new LinkedList<InputReference>();
	}

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
	


}
