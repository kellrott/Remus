package org.remus.js;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.EcmaError;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.WrapFactory;
import org.remus.mapred.MapReduceCallback;
import org.remus.mapred.MapReduceFunction;
import org.remus.mapred.NotSupported;

public class JSFunctionCall implements MapReduceFunction {

	Context cx;
	private JSFunction currentFunction;	

	public JSFunctionCall() {
		cx = Context.enter();
		cx.setWrapFactory( new WrapFactory() {
			@SuppressWarnings("unchecked")

			@Override
			public Scriptable wrapAsJavaObject(Context cx, Scriptable scope, Object javaObject, Class staticType) {
				//System.out.println( javaObject );
				if (javaObject instanceof Map) {
					return new ScriptMap((Map) javaObject);
				}
				if (javaObject instanceof List) {
					return new NativeArray(((List) javaObject).toArray());
				}
				if (javaObject instanceof JSONObject) {
					JSONObject jobj = (JSONObject) javaObject;
					return new ScriptMap(jobj);
				}
				return super.wrapAsJavaObject(cx, scope, javaObject, staticType);
			};


			@Override
			public Scriptable wrapNewObject(Context cx, Scriptable scope,
					Object obj) {
				System.err.println( "NEW:" + obj );
				return super.wrapNewObject(cx, scope, obj);
			}

			@Override
			public Object wrap(Context cx, Scriptable scope, Object obj,
					Class staticType) {
				Object out = super.wrap(cx, scope, obj, staticType);
				//System.err.println( "WRAP:" + obj + " " + staticType + " " + out);
				return out;
			}

		});					
	}


	class JSFunction  {
		public Function func;
		public EmitInterface emit;
		public void call(String key, Object val) {
			Scriptable scope = cx.initStandardObjects();
			cx = Context.enter();
			func.call(cx, scope, null, new Object [] { key, val });
		}
	}

	private JSFunction compileFunction(String source, String fileName) {
		try {
			Scriptable scope = cx.initStandardObjects();
			cx = Context.enter();
			JSFunction jfunc = new JSFunction();
			jfunc.emit = new EmitInterface();
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



	public class EmitInterface {
		MapReduceCallback cb;
		public EmitInterface() {
		}
		private void setEmit(MapReduceCallback cb) {
			this.cb = cb;
		}
		private Object unwrap(Object in) {
			Object out = in;
			if (in instanceof NativeObject) {
				NativeObject n = (NativeObject) in;
				Map m = new HashMap();

				for (Object key : n.getIds()) {
					if (key instanceof String) {
						m.put(key, n.get((String) key, null));
					} else if (key instanceof Integer) {
						m.put(key, n.get((Integer) key, null));
					}
				}
				out = m;
			}
			return out;
		}
		public void emit(String key, Object val) {
			cb.emit(key, unwrap(val));
		}		
	}

	@Override
	public void init(Map instanceInfo) {
		String jsCode = (String)instanceInfo.get("jsCode");
		currentFunction = compileFunction(jsCode, "view");
	}

	@Override
	public void map(String key, Object value, MapReduceCallback cb)
	throws NotSupported {
		currentFunction.emit.setEmit(cb);
		currentFunction.call(key, value);
	}

	@Override
	public void match(String key, List<Object> leftVals,
			List<Object> rightVals, MapReduceCallback cb) throws NotSupported {
		throw new NotSupported();
	}

	@Override
	public void merge(String leftKey, List<Object> leftVals, String rightKey,
			List<Object> rightVals, MapReduceCallback cb) throws NotSupported {
		throw new NotSupported();		
	}

	@Override
	public void pipe(List<Object> handles, MapReduceCallback cb)
	throws NotSupported {
		throw new NotSupported();
	}

	@Override
	public void reduce(String key, List<Object> values, MapReduceCallback cb)
	throws NotSupported {
		throw new NotSupported();
	}

	@Override
	public void split(Object info, MapReduceCallback cb) throws NotSupported {
		throw new NotSupported();
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
