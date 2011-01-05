package org.semweb.scripting;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.EcmaError;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.WrapFactory;
import org.semweb.config.ScriptingConfig;

public class JSInterface implements ScriptingInterface {
	Context cx;	
	@Override
	public void init(ScriptingConfig config) {
		cx = Context.enter();
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

	OutputStream curOUT;

	@Override
	public void addInterface(String name, Object obj) {
		//Object wrappedOut = Context.javaToJS(obj, scope);
		//ScriptableObject.putProperty(scope, name, wrappedOut);				
	}

	@Override
	public void eval(String source, String fileName) {
		try {
			Scriptable scope = cx.initStandardObjects();
			cx = Context.enter();
			FakeDOM dom = new FakeDOM();
			prepScope(scope, dom);
			//System.out.println( source );
			Object out = cx.evaluateString(scope, source, fileName, 1, null);
			//System.out.println( out );
			//System.out.println( out );
			if ( out instanceof ScriptableObject ) {
				curOUT.write(  JSONUtils.toJSONString(out).getBytes() );
			}
		} catch (EcmaError e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}


	class JSFunction implements ScriptingFunction {

		public Function func;
		public FakeDOM dom;

		@Override
		public String call(Object val) {
			Scriptable scope = cx.initStandardObjects();
			cx = Context.enter();
			func.call(cx, scope, null, new Object [] { val } );
			return dom.toString();
		}

	}

	@Override
	public ScriptingFunction compileFunction(String source, String fileName) {
		try {
			Scriptable scope = cx.initStandardObjects();
			cx = Context.enter();
			JSFunction jfunc = new JSFunction();
			jfunc.dom = new FakeDOM();
			prepScope(scope, jfunc.dom);
			Function out = cx.compileFunction(scope, source, fileName, 1, null);
			jfunc.func = out;
			return jfunc;
		} catch (EcmaError e) {
			e.printStackTrace();
		} 
		return null; 
	}

	FakeDOM prepScope(Scriptable scope, FakeDOM dom) {
		Object wrappedOut = Context.javaToJS(dom, scope);
		ScriptableObject.putProperty(scope, "document", wrappedOut);
		return dom;
	}

	@Override
	public void setStdout(OutputStream os) {
		curOUT = os;
		//Object wrappedOut = Context.javaToJS( new PrintWriter(os), scope);
		//ScriptableObject.putProperty(scope, "stdout", wrappedOut);
	}

	static public class FakeDOM {
		StringBuilder out;
		public FakeDOM() {
			this.out = new StringBuilder();
		}

		public void write(String str) {
			out.append( str );
		}
		@Override
		public String toString() {
			return out.toString();
		}
	}

}
