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
import org.mozilla.javascript.WrapFactory;

public class JSRunner {

	Context cx;
	Scriptable scope;
	NoteInterface note;
	
	public JSRunner() {
		cx = Context.enter();
		scope = cx.initStandardObjects();
		
		cx.setWrapFactory( new WrapFactory() {
			@SuppressWarnings("unchecked")
			@Override
			public Scriptable wrapAsJavaObject(Context cx, Scriptable scope, java.lang.Object javaObject, java.lang.Class<?> staticType) {
				if ( javaObject instanceof Map ) {
					return new ScriptMap( (Map)javaObject );
				}
				return super.wrapAsJavaObject(cx, scope, javaObject, staticType);
			}
		});
		
	}
	
	
	public void addInterface(String name, Object obj) {
		Object wrappedOut = Context.javaToJS(obj, scope);
		ScriptableObject.putProperty(scope, name, wrappedOut);		
	}

	public String eval(String code, String fileName, PageInterface page ) {
		try {
			cx = Context.enter();
			//System.err.println("EVAL=" + code );
			if ( page != null )
				addInterface("page", page );

			Object result = cx.evaluateString(scope, code, fileName, 1, null);			
			
			return Context.toString( result );
			//System.err.print("RES=" + outBuffer.toString() );
		} catch (EcmaError e) {
			e.printStackTrace();
		} 
		return "";
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
		js.addInterface("note", new NoteInterface("test_rdf"));
		PageInterface page = PageInterface.newInstance();
		System.err.println( js.eval( sb.toString(), args[0], page ) );	
		System.out.println( page.outStream.toString() );
		System.out.println( page.paramMap );
	}


}
