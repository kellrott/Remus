package org.remus;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RemusPath {
	private String printURL = null;
	
	private String appletView = null;
	private String appletName = null;
	private String appletPortName = null;
	private String instance = null;
	
	private String fileName = null;

	int input_type;
	public static final int AppletInput = 0;
	public static final int DynamicInput = 1;
	public static final int ExternalInput = 2;
	public static final int StaticInput = 2;
	

	Pattern appletSub = Pattern.compile("(\\:\\w+)\\.(\\w+)$");
	Pattern instancePat = Pattern.compile("^([^/]*)/([^/]*)$");
	Pattern instanceKeyPat = Pattern.compile("^([^/]*)/([^/]*)/(.*)$");

	private String path;
	private String key;
	private File file, srcFile;

	public RemusPath( RemusApp parent, String pathinfo ) {
		String [] tmp = pathinfo.split("@");
		path = tmp[0];
		appletView = null;
		instance = null;
		appletPortName = null;
		key = null;
		if ( tmp.length > 1 ) {
			Matcher m1 = instancePat.matcher(tmp[1]);
			if ( m1.find() ) {
				appletView = m1.group(1);
				instance = m1.group(2);
			} else {
				Matcher m2 = instanceKeyPat.matcher(tmp[1]); 
				if ( m2.find() ) {
					appletView = m2.group(1);
					instance = m2.group(2);
					key = m2.group(3);
				} else {
					appletView = tmp[1];
				}
			}
		}
		if ( appletView != null && appletView.length() == 0 )
			appletView = null;
		Matcher m = appletSub.matcher( tmp[0] );
		if ( m.find() ) {
			String appletName = m.group(1);
			appletPortName = m.group(2);
			appletName = m.replaceAll(appletName);
		} else {
			appletName = tmp[0];
		}
		appletName = (new File(appletName)).getAbsolutePath();
		file = new File( path );	
		path = file.getAbsolutePath();
		srcFile = new File(parent.srcbase, path );		
	}


	RemusApp parent;
	public RemusPath(RemusApp parent, String url, String reqPath) throws FileNotFoundException {
		this.parent = parent;
		if ( url.startsWith("http://") || url.startsWith("https://") ) {
			printURL = url;
		} else {

			if ( !url.startsWith("/") && url.compareTo("?")!=0 && url.compareTo("$")!=0) {
				if ( url.startsWith(":") ) {
					String localFile = new File( parent.getSrcBase(), reqPath ).getAbsolutePath().replaceFirst( parent.getSrcBase().toString(), "" );
					url = localFile + url;
				} else {
					String localFile = new File( parent.getSrcBase(), reqPath ).getParentFile().getAbsolutePath().replaceFirst( parent.getSrcBase().toString(), "" );
					url = localFile + "/" + url;
				}
			}				
			if ( url.contains(":") ) {
				if ( url.contains("@") ) {
					String [] tmp = url.split("@");
					url = tmp[0];
					appletView = tmp[1];					
				} else {
					appletView = "data";
				}
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
			} else if ( url.compareTo("$") == 0) {
				appletName = null;
				printURL = "$";
				input_type=StaticInput;
			} else {
				File localFile = new File( parent.getSrcBase(), url );
				if ( !localFile.exists() ) {
					throw new FileNotFoundException(localFile.getAbsolutePath());
				}
				printURL = url;
				input_type=ExternalInput;
			}
			
		}
	}
	

	public int getInputType() {
		return input_type;
	}
	
	public String getPortPath() {
		if ( appletPortName != null )
			return appletName + "." + appletPortName;
		return appletName;
	}
	
	public String getViewPath() {
		if ( appletPortName != null )
			return appletName + "." + appletPortName + "@" + appletView;
		return appletName + "@" + appletView;
	}
	
	
	public boolean isPortName() {
		if ( appletPortName == null )
			return false;
		return true;
	}
	
	public String getURL() {
		return printURL;
	}


	public String getAppletPath() {
		return appletName;
	}


	public String getView() {
		return appletView;
	}

	public String getPortName() {
		return appletPortName;
	}


	public String getInstance() {
		return instance;
	}


	public String getKey() {
		return key;
	}

	
	public File getSrcFile() {
		return srcFile;
	}
}
