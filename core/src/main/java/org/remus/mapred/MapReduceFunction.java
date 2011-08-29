package org.remus.mapred;

import java.util.List;
import java.util.Map;

public interface MapReduceFunction {
	
	void init(Map instanceInfo);	
	void split(Object info, MapReduceCallback cb) throws NotSupported, Exception;
	void map(String key, Object value, MapReduceCallback cb) throws NotSupported, Exception;
	void reduce(String key, List<Object> values, MapReduceCallback cb) throws NotSupported, Exception;
	void merge(String left_key, List<Object> left_vals,
			String right_key, List<Object> right_vals, MapReduceCallback cb) throws NotSupported, Exception;
	void match(String key, List<Object> left_vals,
			List<Object> right_vals, MapReduceCallback cb) throws NotSupported, Exception;
	void pipe(List<Object> handles, MapReduceCallback cb) throws NotSupported, Exception;
	void cleanup();
}
