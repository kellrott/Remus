package org.remus.mapred;

import java.io.InputStream;


public interface SpliterInterface extends InterfaceBase {

	void initSpliter(String config);
	void split( InputStream input, SplitCallback callback );
	
}
