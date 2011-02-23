package org.remus.applet;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.remus.RemusApp;
import org.remus.RemusInstance;
import org.remus.RemusPath;
import org.remus.WorkDescription;
import org.remus.WorkReference;

public class PipeGenerator implements WorkGenerator {
	ArrayList<WorkDescription> outList;
	int curPos;
	RemusApplet applet;
	@Override
	public void init(RemusApplet applet) {
		this.applet = applet;
	}

	@Override
	public void startWork(RemusInstance instance, long reqCount) {
		outList = new ArrayList<WorkDescription>();
		Map out = new HashMap();
		List a = new LinkedList();
		for ( int i = 0; i < applet.inputs.size(); i++ ) {
			RemusPath iRef = new RemusPath(applet.inputs.get(i), instance);
			a.add( iRef.getInstancePath() );
		}
		out.put( "input", a );
		outList.add( new WorkDescription( new WorkReference(applet, instance, 0), out) );
		curPos = 0;
	}		

	@Override
	public WorkDescription nextWork() {
		if ( curPos < outList.size() ) {
			curPos++;
			return outList.get(curPos-1);
		}
		return null;
	}



}
