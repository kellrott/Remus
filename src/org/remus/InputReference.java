package org.remus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;


public class InputReference {
	boolean isLocal;
	String inputHREF;
	String srcPage;
	String finalURL;
	File localFile;
	String localID = null;
	CodeManager parent;
	public InputReference(CodeManager parent, String url, File reqPage) throws FileNotFoundException {
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
				localFile = new File( parent.parent.srcbase, fileTest );
				if ( !localFile.exists() ) {
					throw new FileNotFoundException(localFile.getAbsolutePath());
				}
				localID = idTest;
				finalURL = url;
			} else if ( url.startsWith(":") ) {
				localFile = reqPage;
				localID = idTest;
				finalURL = localFile.getAbsolutePath().replaceFirst( parent.parent.getSrcBase().toString(), "" ) + ":" + localID;
			} else {
				localFile = new File( reqPage.getParentFile(), fileTest);
				if ( !localFile.exists() ) {
					throw new FileNotFoundException(localFile.getAbsolutePath());
				}
				localID = idTest;
				if ( localID == null ) 
					finalURL = localFile.getAbsolutePath().replaceFirst( parent.parent.getSrcBase().toString(), "");
				else
					finalURL = localFile.getAbsolutePath().replaceFirst( parent.parent.getSrcBase().toString(), "") + ":" + localID;
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
