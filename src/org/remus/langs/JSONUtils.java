package org.remus.langs;

/**
 * Copyright (C) 2005-2009 Alfresco Software Limited.
 *
 * This file is part of the Spring Surf Extension project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.json.JSONWriter;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.IdScriptableObject;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.NativeObject;


/**
 * Collection of JSON Utility methods.
 * This class is immutable.
 * 
 * @author Roy Wetherall
 */


public class JSONUtils {


	/**
	 * Converts a given JavaScript native object and converts it to the relevant JSON string.
	 * 
	 * @param object            JavaScript object
	 * @return String           JSON      
	 * @throws JSONException
	 */
	
	static public String toJSONString(Object object)
	throws JSONException
	{
		JSONStringer json = new JSONStringer();

		if (object instanceof NativeArray)
		{
			nativeArrayToJSONString((NativeArray)object, json);
		}
		else if (object instanceof NativeObject)
		{ 
			nativeObjectToJSONString((NativeObject)object, json);
		}
		else
		{
			// TODO what else should this support?
		}        

		return json.toString();
	}

	/**
	 * Takes a JSON string and converts it to a native java script object
	 * 
	 * @param  jsonString       a valid json string
	 * @return NativeObject     the created native JS object that represents the JSON object
	 * @throws JSONException    
	 */
	
	static public NativeObject toObject(String jsonString)
	throws JSONException
	{
		// TODO deal with json array stirngs

		// Parse JSON string
		JSONObject jsonObject = new JSONObject(jsonString);

		// Create native object 
		return toObject(jsonObject);
	}

	/**
	 * Takes a JSON object and converts it to a native JS object.
	 * 
	 * @param jsonObject        the json object
	 * @return NativeObject     the created native object
	 * @throws JSONException
	 */
	
	static public NativeObject toObject(JSONObject jsonObject)
	throws JSONException
	{
		// Create native object 
		NativeObject object = new NativeObject();

		Iterator<String> keys = jsonObject.keys();
		while (keys.hasNext())
		{
			String key = (String)keys.next();
			Object value = jsonObject.get(key);
			if (value instanceof JSONObject)
			{
				object.put(key, object, toObject((JSONObject)value));
			}
			else
			{
				object.put(key, object, value);
			}
		}

		return object;
	}

	/**
	 * Build a JSON string for a native object
	 * 
	 * @param nativeObject
	 * @param json
	 * @throws JSONException
	 */
	static private void nativeObjectToJSONString(NativeObject nativeObject, JSONStringer json)
	throws JSONException
	{
		json.object();

		Object[] ids = nativeObject.getIds();
		for (Object id : ids)
		{
			String key = id.toString();
			json.key(key);

			Object value = nativeObject.get(key, nativeObject);
			valueToJSONString(value, json);
		}

		json.endObject();
	}

	/**
	 * Build JSON string for a native array
	 * 
	 * @param nativeArray
	 * @param json
	 */
	static private void nativeArrayToJSONString(NativeArray nativeArray, JSONStringer json)
	throws JSONException
	{
		Object[] propIds = nativeArray.getIds();
		if (isArray(propIds) == true)
		{      
			json.array();

			for (int i=0; i<propIds.length; i++)
			{
				Object propId = propIds[i];
				if (propId instanceof Integer)
				{
					Object value = nativeArray.get((Integer)propId, nativeArray);
					valueToJSONString(value, json);
				}
			}

			json.endArray();
		}
		else
		{
			json.object();

			for (Object propId : propIds)
			{
				Object value = nativeArray.get(propId.toString(), nativeArray);
				json.key(propId.toString());
				valueToJSONString(value, json);    
			}            

			json.endObject();
		}
	}

	/**
	 * Look at the id's of a native array and try to determine whether it's actually an Array or a HashMap
	 * 
	 * @param ids       id's of the native array
	 * @return boolean  true if it's an array, false otherwise (ie it's a map)
	 */
	static private boolean isArray(Object[] ids)
	{
		boolean result = true;
		for (Object id : ids)
		{
			if (id instanceof Integer == false)
			{
				result = false;
				break;
			}
		}
		return result;
	}

	/**
	 * Convert value to JSON string
	 * 
	 * @param value
	 * @param json
	 * @throws JSONException
	 */
	static private void valueToJSONString(Object value, JSONStringer json)
	throws JSONException
	{
		if (value instanceof IdScriptableObject &&
				((IdScriptableObject)value).getClassName().equals("Date") == true)
		{
			// Get the UTC values of the date
			Object year = NativeObject.callMethod((IdScriptableObject)value, "getUTCFullYear", null);
			Object month = NativeObject.callMethod((IdScriptableObject)value, "getUTCMonth", null);
			Object date = NativeObject.callMethod((IdScriptableObject)value, "getUTCDate", null);
			Object hours = NativeObject.callMethod((IdScriptableObject)value, "getUTCHours", null);
			Object minutes = NativeObject.callMethod((IdScriptableObject)value, "getUTCMinutes", null);
			Object seconds = NativeObject.callMethod((IdScriptableObject)value, "getUTCSeconds", null);
			Object milliSeconds = NativeObject.callMethod((IdScriptableObject)value, "getUTCMilliseconds", null);

			// Build the JSON object to represent the UTC date
			json.object()
			.key("zone").value("UTC")
			.key("year").value(year)
			.key("month").value(month)
			.key("date").value(date)
			.key("hours").value(hours)
			.key("minutes").value(minutes)
			.key("seconds").value(seconds)
			.key("milliseconds").value(milliSeconds)
			.endObject();

		}
		else if (value instanceof NativeJavaObject)
		{
			Object javaValue = Context.jsToJava(value, Object.class);
			json.value(javaValue);
		}
		else if (value instanceof NativeArray)
		{
			// Output the native object
			nativeArrayToJSONString((NativeArray)value, json);
		}
		else if (value instanceof NativeObject)
		{
			// Output the native array
			nativeObjectToJSONString((NativeObject)value, json);
		}
		else
		{
			json.value(value);
		}
	}    	
}

