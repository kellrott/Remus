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
	
	int input_type;
	public static final int AppletInput = 0;
	public static final int DynamicInput = 1;
	public static final int StaticInput = 2;

	Pattern appletSub = Pattern.compile("(\\:\\w+)\\.(\\w+)$");

	RemusApp parent;
	public InputReference(RemusApp parent, String url, String reqPath) throws FileNotFoundException {
		this.parent = parent;
		if ( url.startsWith("http://") || url.startsWith("https://") ) {
			printURL = url;
		} else {

			if ( !url.startsWith("/") && url.compareTo("?")!=0) {
				if ( url.startsWith(":") ) {
					String localFile = new File( parent.getSrcBase(), reqPath ).getAbsolutePath().replaceFirst( parent.getSrcBase().toString(), "" );
					url = localFile + url;
				} else {
					String localFile = new File( parent.getSrcBase(), reqPath ).getParentFile().getAbsolutePath().replaceFirst( parent.getSrcBase().toString(), "" );
					url = localFile + "/" + url;
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
				input_type=AppletInput;
			} else if ( url.compareTo("?") == 0) {
				appletName = null;
				printURL = url;
				input_type=DynamicInput;
			} else {
				File localFile = new File( parent.getSrcBase(), url );
				if ( !localFile.exists() ) {
					throw new FileNotFoundException(localFile.getAbsolutePath());
				}
				printURL = url;
				input_type=StaticInput;
			}
			
		}
	}
	
	

	public int getInputType() {
		return input_type;
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
