package org.remus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;

public class InputReference {
	String finalURL;
	File localFile = null;
	String localID = null;
	Boolean dynamicInput = false;

	RemusApp parent;
	public InputReference(RemusApp parent, String url, String reqPath) throws FileNotFoundException {
		this.parent = parent;
		if ( url.startsWith("http://") || url.startsWith("https://") ) {
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
				localFile = new File( parent.srcbase, fileTest );
				if ( !localFile.exists() ) {
					throw new FileNotFoundException(localFile.getAbsolutePath());
				}
				localID = idTest;
				finalURL = url;
			} else if ( url.startsWith(":") ) {
				localFile = new File( parent.getSrcBase(), reqPath );
				localID = idTest;
				finalURL = localFile.getAbsolutePath().replaceFirst( parent.getSrcBase().toString(), "" ) + ":" + localID;
			} else if ( url.compareTo("?") == 0) {
				localFile = null;
				localID = null;
				finalURL = null;
				dynamicInput = true;
			} else {
				localFile = new File( (new File( parent.getSrcBase(), reqPath )).getParentFile(), fileTest);
				if ( !localFile.exists() ) {
					throw new FileNotFoundException(localFile.getAbsolutePath());
				}
				localID = idTest;
				if ( localID == null ) 
					finalURL = localFile.getAbsolutePath().replaceFirst( parent.getSrcBase().toString(), "");
				else
					finalURL = localFile.getAbsolutePath().replaceFirst( parent.getSrcBase().toString(), "") + ":" + localID;
			}
		}
	}
	
	
	public Boolean isLocal() {
		if ( localFile == null )
			return false;
		return true;
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

	public String getPath() {
		if ( dynamicInput )
			return "?";
		return finalURL.toString();
	}

}
