package org.semweb.scripting;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.EcmaError;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.WrapFactory;
import org.semweb.config.ScriptingConfig;

public class JSInterface implements ScriptingInterface {

	Context cx;
	Scriptable scope;
	
	@Override
	public void init(ScriptingConfig config) {
		cx = Context.enter();
		scope = cx.initStandardObjects();
		cx.setWrapFactory( new WrapFactory() {
			@SuppressWarnings("unchecked")
			@Override
			public Scriptable wrapAsJavaObject(Context cx, Scriptable scope, java.lang.Object javaObject, java.lang.Class<?> staticType) {
				//System.out.println( javaObject );
				if ( javaObject instanceof Map ) {
					return new ScriptMap( (Map)javaObject );
				}
				if ( javaObject instanceof List ) {
					return new NativeArray( ((List)javaObject).toArray() );
				}
				return super.wrapAsJavaObject(cx, scope, javaObject, staticType);
			}
		});		

	}
	
	@Override
	public void addInterface(String name, Object obj) {
		Object wrappedOut = Context.javaToJS(obj, scope);
		ScriptableObject.putProperty(scope, name, wrappedOut);				
	}

	@Override
	public void eval(String source, String fileName) {
		try {
			cx = Context.enter();
			System.out.println( source );
			cx.evaluateString(scope, source, fileName, 1, null);
		} catch (EcmaError e) {
			e.printStackTrace();
		} 
	}

	@Override
	public void setStdout(OutputStream os) {
		Object wrappedOut = Context.javaToJS( new PrintWriter(os), scope);
		ScriptableObject.putProperty(scope, "stdout", wrappedOut);
	}

}
