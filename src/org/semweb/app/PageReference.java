package org.semweb.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;

public class PageReference {
	boolean isLocal;
	String inputHREF;
	String srcPage;
	String finalURL;
	File localFile;
	String localID = null;
	PageManager parent;
	public PageReference(PageManager parent, String url, File reqPage) throws FileNotFoundException {
		this.parent = parent;
		inputHREF = url;
		if ( url.startsWith("http://") || url.startsWith("https://") ) {
			isLocal = false;
			finalURL = url;			
		} else {
			String fileTest = url;
			String idTest = null;
			if ( url.contains(":" ) ) {
				String [] tmp = url.split(":");
				fileTest = tmp[0];
				idTest = tmp[1];
			}
			if ( fileTest.startsWith("/") ) {				
				localFile = new File( parent.parent.appBase, fileTest + PageParser.PageExt);
				if ( !localFile.exists() ) {
					throw new FileNotFoundException(localFile.getAbsolutePath());
				}
				localID = idTest;
				finalURL = url;
			} else if ( url.startsWith(":") ) {
				localFile = reqPage;
				localID = idTest;
				finalURL = localFile.getAbsolutePath().replaceFirst( parent.parent.getPageBase().toString(), "").replaceFirst(PageParser.PageExt + "$", "") + ":" + localID;
			} else {
				localFile = new File( reqPage.getParentFile(), fileTest);
				if ( !localFile.exists() ) {
					throw new FileNotFoundException(localFile.getAbsolutePath());
				}
				localID = idTest;
				if ( localID == null ) 
					finalURL = localFile.getAbsolutePath().replaceFirst( parent.parent.getPageBase().toString(), "").replaceFirst(PageParser.PageExt + "$", "");
				else
					finalURL = localFile.getAbsolutePath().replaceFirst( parent.parent.getPageBase().toString(), "").replaceFirst(PageParser.PageExt + "$", "") + ":" + localID;
				
			}
		}		

	}

	public String getURL() {
		return finalURL.toString();
	}
	
	public File getLocalFile() {
		return localFile;
	}
	
	public String getElementID() {
		return localID;
	}
	
	public Serializable getContent() {
		//PageRequest page = parent.openPage( localFile.getAbsolutePath() );
		//return page.open();
		return "pageContents";
	}

}
