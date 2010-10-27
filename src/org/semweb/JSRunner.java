package org.semweb;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
	ByteArrayOutputStream outBuffer;
	PrintWriter outWriter;
	
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

		outBuffer = new ByteArrayOutputStream();
		outWriter = new PrintWriter(outBuffer);
		
		Object wrappedOut = Context.javaToJS(outWriter, scope);
		ScriptableObject.putProperty(scope, "out", wrappedOut);

	}
	
	
	public void addInterface(String name, Object obj) {
		ScriptableObject.putProperty(scope, name, obj);		
	}


	public String eval(String code, Boolean outPrint) {
		outBuffer.reset();
		try {
			//System.err.println("EVAL=" + code );
			Object result = cx.evaluateString(scope, code, "<cmd>", 1, null);			
			if ( !outPrint ) {
				return Context.toString( result );
			} else {
				outBuffer.flush();
				outWriter.flush();
				return outBuffer.toString();
			}
			//System.err.print("RES=" + outBuffer.toString() );
		} catch (EcmaError e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
		js.eval( sb.toString(), true );

	}


}
