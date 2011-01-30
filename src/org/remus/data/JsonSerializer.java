package org.remus.data;

import java.util.List;
import java.util.Map;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

import org.json.JSONException;

public class JsonSerializer implements Serializer {

	@Override
	public String dumps(Object o) {

			if ( o instanceof List ) {
				StringBuilder sb = new StringBuilder("[");
				boolean first = true;
				for ( Object c : (List)o ) {
					if ( !first )
						sb.append(",");
					else
						first = false;
					sb.append( dumps(c) );
				}
				sb.append("]");
				return sb.toString();
			} else if ( o instanceof Map ) {
				StringBuilder sb = new StringBuilder("{");
				boolean first = true;
				for ( Object key : ((Map)o).keySet() ) {
					if ( !first )
						sb.append(",");
					else
						first = false;
					sb.append( dumps(key) );
					sb.append(":");
					sb.append( dumps(((Map)o).get(key)) );
				}
				sb.append("}");
				return sb.toString();
			} else if ( o instanceof String ) {
				return "\"" + ((String)o) + "\"";
			} else if ( o instanceof Float ) {
				return Float.toString((Float)o);
			} else if ( o instanceof Integer ) {
				return Integer.toString(((Integer)o));
			}
		return null;
	}

	@Override
	public Object loads(String s) {
		try {
			return JSONUtils.toObject(s);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
