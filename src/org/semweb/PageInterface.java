package org.semweb;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeArray;

public class PageInterface {

	static PageInterface newInstance() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		return new PageInterface(out);
	}

	Map<String,Object> paramMap;
	ByteArrayOutputStream outStream;
	
	public PageInterface( ByteArrayOutputStream out ) {
		paramMap = new HashMap<String,Object>();
		outStream = out;
	}
	
	public void reset() {
		outStream.reset();
	}
	
	@Override
	public String toString() {		
		try {
			outStream.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return outStream.toString();
	}
	
	public void println(String x) {
		print(x);
		print("\n");
	}
	
	public void print(String x) {
		try {
			outStream.write(x.getBytes());
			//outStream.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		System.err.println( outStream.toString() );
	}	

	public Object get(String key) {
		return paramMap.get(key);
	}

	public void put(String key, Object value) {
		if ( value instanceof NativeArray ) {
			NativeArray na = (NativeArray)value;
			paramMap.put( key , Context.jsToJava(na, (new Object[0]).getClass() ) );
		} else {
			paramMap.put(key,value);
		}
	}

	public Object remove(Object key) {
		return paramMap.remove(key);
	}
}
