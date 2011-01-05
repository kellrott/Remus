package org.semweb.scripting;

import java.io.OutputStream;

import org.semweb.config.ScriptingConfig;


public interface ScriptingInterface {

	public void init(ScriptingConfig config);
	public void setStdout(OutputStream os);
	public void eval(String source, String fileName);
	public ScriptingFunction compileFunction( String source, String fileName );
	public void addInterface(String name, Object obj );
		
}
