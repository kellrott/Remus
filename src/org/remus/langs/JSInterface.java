package org.remus.langs;

import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.EcmaError;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.FunctionObject;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.WrapFactory;
import org.remus.PluginConfig;
import org.remus.mapred.MapCallback;
import org.remus.mapred.MapperInterface;
import org.remus.mapred.ReducerInterface;
import org.remus.mapred.SplitCallback;
import org.remus.mapred.SpliterInterface;
import org.remus.mapred.PipeInterface;
import org.remus.serverNodes.BaseStackNode;

public class JSInterface implements MapperInterface {
	Context cx;	

	public void init(PluginConfig config) {
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
			
			@Override
			public Scriptable wrapNewObject(Context cx, Scriptable scope,
					Object obj) {
				System.err.println( "NEW:" + obj );
				return super.wrapNewObject(cx, scope, obj);
			}
			@Override
			public Object wrap(Context cx, Scriptable scope, Object obj,Class<?> staticType) {
				Object out = super.wrap(cx, scope, obj, staticType);
				//System.err.println( "WRAP:" + obj + " " + staticType + " " + out);
				return out;
			}
		});		
	}

	OutputStream curOUT;
	private JSFunction currentFunction;


	class JSFunction  {
		public Function func;
		public EmitInterface emit;
		public void call(String key, Object val) {
			Scriptable scope = cx.initStandardObjects();
			cx = Context.enter();
			func.call(cx, scope, null, new Object [] { key, val } );
		}
	}

	private JSFunction compileFunction(String source, String fileName) {
		try {
			Scriptable scope = cx.initStandardObjects();
			cx = Context.enter();
			JSFunction jfunc = new JSFunction();
			jfunc.emit = new EmitInterface(  );
			Function out = cx.compileFunction(scope, source, fileName, 1, null);
			jfunc.func = out;
			prepScope(scope, jfunc.emit);
			
			return jfunc;
		} catch (EcmaError e) {
			e.printStackTrace();
		} 
		return null; 
	}

	void prepScope(Scriptable scope, EmitInterface emit) {		
		try {
			//Class[] parameters = new Class[] { String.class, Object.class };
			ScriptableObject.putProperty(scope, "remus", Context.javaToJS(emit, scope));
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setStdout(OutputStream os) {
		curOUT = os;
		//Object wrappedOut = Context.javaToJS( new PrintWriter(os), scope);
		//ScriptableObject.putProperty(scope, "stdout", wrappedOut);
	}

	public class EmitInterface {
		MapCallback cb;
		public EmitInterface() {
		}
		private void setEmit(MapCallback cb) {
			this.cb = cb;
		}
		private Object unwrap(Object in) {
			Object out = in;
			if ( in instanceof NativeObject ) {
				NativeObject n = (NativeObject)in;
				Map m = new HashMap();

				for ( Object key : n.getIds() ) {
					if ( key instanceof String )
						m.put(key, n.get((String)key, null));
					else if ( key instanceof Integer)
						m.put(key, n.get((Integer)key, null));
							
				}
				out = m;
			}
			return out;
		}
		public void emit(String key, Object val) {
			cb.emit(key, unwrap(val) );
		}		
	}


	@Override
	public void map(BaseStackNode dataStack, MapCallback callback) {
		currentFunction.emit.setEmit(callback);
		for (String key : dataStack.getKeys() ) {
			for (Object data : dataStack.getData(key)) {
				currentFunction.call( key, data );
			}
		}
	}

	@Override
	public void initMapper(String config) {
		currentFunction = compileFunction(config, "view");
	}

}
