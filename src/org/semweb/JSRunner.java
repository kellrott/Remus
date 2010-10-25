package org.semweb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;


import org.mozilla.javascript.Context;
import org.mozilla.javascript.EcmaError;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.WrapFactory;

public class JSRunner {

	Context cx;
	Scriptable scope;
	NoteInterface note;

	public JSRunner() {

		cx = Context.enter();
		scope = cx.initStandardObjects();

		cx.setWrapFactory( new WrapFactory() {
			/*
			@Override
			public Object wrap(Context cx, Scriptable scope, Object obj,
					Class<?> staticType)
			{
				System.err.println( obj + " " + staticType );
				if (obj instanceof String || obj instanceof Number ||
						obj instanceof Boolean)
				{
					return obj;
				} else if (obj instanceof Character) {
					char[] a = { ((Character)obj).charValue() };
					return new String(a);
				}
				return super.wrap(cx, scope, obj, staticType);
			}
			 */
			@Override
			public Scriptable wrapAsJavaObject(Context cx, Scriptable scope, java.lang.Object javaObject, java.lang.Class<?> staticType) {
				if ( javaObject instanceof Map ) {
					/*
					ScriptableObject out = new ScriptableObject() {						
						@Override
						public String getClassName() {
							return "ScriptMap";
						}
					};
					Map map = ((Map) javaObject);
					for ( Object key : map.keySet() ) {
						System.out.println("wrapkey: "+ key.toString() );
						out.put(key.toString(), null, map.get(key));
					}
					return out;
				*/
					return new ScriptMap( (Map)javaObject );
				}
				return super.wrapAsJavaObject(cx, scope, javaObject, staticType);
			}			
		});

		
		Object wrappedOut = Context.javaToJS(System.out, scope);
		ScriptableObject.putProperty(scope, "out", wrappedOut);

		note = new NoteInterface("test_db");
		ScriptableObject.putProperty(scope, "note", note);

	}


	public void runScript( String script ) {
		try {
			Object result = cx.evaluateString(scope, script, "<cmd>", 1, null);
			//System.out.println();
			//System.out.print(cx.toString( result ));		
		} catch (EcmaError e) {
			e.printStackTrace();
		}
	}


	public static void main(String []args) throws IOException {

		BufferedReader br = new BufferedReader( new InputStreamReader(  new FileInputStream(new File(args[0])) ));
		StringBuilder sb = new StringBuilder();
		String line;
		do {
			line = br.readLine();
			if ( line != null )
				sb.append(line);
		} while (line != null);

		JSRunner js = new JSRunner();
		js.runScript( sb.toString() );

	}
}
