package org.remus.langs;

import java.util.Map;

import org.mozilla.javascript.Scriptable;

@SuppressWarnings("unchecked")
public class ScriptMap implements Scriptable {

	Map map;
	public ScriptMap(Map map) {
		this.map = map;
	}
	
	@Override
	public void delete(String arg0) {
		map.remove(arg0);
	}

	@Override
	public void delete(int arg0) {
		
	}

	@Override
	public Object get(String arg0, Scriptable arg1) {
		return map.get(arg0);
	}

	@Override
	public Object get(int arg0, Scriptable arg1) {
		return null;
	}

	@Override
	public String getClassName() {
		return "class";
	}

	@Override
	public Object getDefaultValue(Class<?> arg0) {		
		return null;
	}

	@Override
	public Object[] getIds() {
		return map.keySet().toArray();
	}

	@Override
	public Scriptable getParentScope() {
		return null;
	}

	@Override
	public Scriptable getPrototype() {
		return null;
	}

	@Override
	public boolean has(String arg0, Scriptable arg1) {
		return map.containsKey(arg0);
	}

	@Override
	public boolean has(int arg0, Scriptable arg1) {
		return false;
	}

	@Override
	public boolean hasInstance(Scriptable arg0) {
		return false;
	}

	@Override
	public void put(String arg0, Scriptable arg1, Object arg2) {
		map.put( arg0, arg2 );
	}

	@Override
	public void put(int arg0, Scriptable arg1, Object arg2) {
	}

	@Override
	public void setParentScope(Scriptable arg0) {
	}

	@Override
	public void setPrototype(Scriptable arg0) {
	}

	@Override
	public String toString() {
		return map.toString();
	}

}
