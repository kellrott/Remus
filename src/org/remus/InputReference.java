package org.remus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InputReference {
	private String printURL = null;
	
	private String appletName = null;
	private String appletPortName = null;
	private String fileName = null;
	
	Boolean appletInput = false;
	Boolean dynamicInput = false;

	Pattern appletSub = Pattern.compile("(\\:\\w+)\\.(\\w+)$");

	RemusApp parent;
	public InputReference(RemusApp parent, String url, String reqPath) throws FileNotFoundException {
		this.parent = parent;
		if ( url.startsWith("http://") || url.startsWith("https://") ) {
			printURL = url;
		} else {

			if ( !url.startsWith("/") ) {
				if ( url.startsWith(":") ) {
					String localFile = new File( parent.getSrcBase(), reqPath ).getAbsolutePath().replaceFirst( parent.getSrcBase().toString(), "" );
					url = localFile + url;
				} else {
					//TODO:do something here
				}
			}
				
			if ( url.contains(":") ) {
				Matcher m = appletSub.matcher( url );
				if ( m.find() ) {
					appletName = m.replaceFirst( m.group(1) );
					appletPortName = m.group(2);
					fileName =  m.replaceFirst("") + ".xml";
				} else {
					String [] tmp = url.split(":");
					fileName = tmp[0] + ".xml";
					appletName = url;
				}
				File localFile = new File( parent.getSrcBase(), fileName );
				if ( !localFile.exists() ) {
					throw new FileNotFoundException(localFile.getAbsolutePath());
				}
				printURL = url;
			} else if ( url.compareTo("?") == 0) {
				appletName = null;
				printURL = url;
				dynamicInput = true;
			} 
			
		}
	}
	
	

	public Boolean isApplet() {
		return appletInput;
	}

	/*
	public String getElementID() {
		return appletName;
	}
	*/
	/*
	public Serializable getContent() {
		//PageRequest page = parent.openPage( localFile.getAbsolutePath() );
		//return page.open();
		return "pageContents";
	}
	 */
	/*
	public String getURL() {
		return parent.baseURL + printURL;
	}
	*/
	
	public String getPortPath() {
		if ( appletPortName != null )
			return appletName + "." + appletPortName;
		return appletName;
	}
	
	public String getURL() {
		return printURL;
	}


	public String getAppletPath() {
		return appletName;
	}

}
