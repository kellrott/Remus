package org.semweb.app;

import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;

public class InputConnection {
	boolean isLocal;
	public String inputHREF;
	public String srcPage;
	URL finalURL;
	File localFile;
	String localID;
	PageManager parent;
	public InputConnection(PageManager parent, String url, File reqPage) {
		this.parent = parent;
		inputHREF = url;
		if ( url.startsWith("http://") || url.startsWith("https://") ) {
			isLocal = false;
			try {
				finalURL = new URL(url);
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			String fileTest = url;
			String idTest = null;
			if ( url.contains(":" ) ) {
				String [] tmp = url.split(":");
				fileTest = tmp[0];
				idTest = tmp[1];
			}
			if ( fileTest.startsWith("/") ) {
				localFile = new File( parent.parent.appBase, fileTest );
			} else {
				localFile = new File( reqPage.getParentFile(), fileTest);
			}
		}		

	}

	public Serializable getContent() {
		PageRequest page = parent.openPage( localFile.getAbsolutePath() );
		//return page.open();
		return "pageContents";
	}

}
