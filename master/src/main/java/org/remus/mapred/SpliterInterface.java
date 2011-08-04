package org.remus.mapred;

import java.io.InputStream;


public interface SpliterInterface {

	void initSpliter(String config);
	void split( InputStream input, SplitCallback callback );
	
}
