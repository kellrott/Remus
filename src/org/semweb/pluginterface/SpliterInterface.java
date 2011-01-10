package org.semweb.pluginterface;

import java.io.InputStream;


public interface SpliterInterface extends InterfaceBase {

	void prepSpliter(String config);
	void split( InputStream input, SplitCallback callback );
	
}
