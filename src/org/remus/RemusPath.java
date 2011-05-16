package org.remus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Map;

import org.mpstore.MPStore;
import org.remus.work.Submission;

/**
 * 
 * @author kellrott
 *
 */

public class RemusPath {
	RemusApp parent;

	private String url = null;

	private String pipelineName = null;
	private String viewName = null;
	private String appletName = null;
	private String appletPortName = null;
	private String instance = null;
	private String key = null;
	private String attachName = null;
	private int input_type;

	public static final int AppletInput = 0;
	public static final int DynamicInput = 1;
	public static final int AttachInput = 2;

	//static final Pattern appletSub = Pattern.compile("(\\:\\w+)\\.(\\w+)$");
	//static final Pattern pipelineAttachment = Pattern.compile("^/([^/]*)/(.*)$");
	//static final Pattern instancePat = Pattern.compile("^([^/]*)/([^/]*)$");
	//static final Pattern instanceKeyPat = Pattern.compile("^([^/]*)/([^/]*)/(.*)$");

	
	public RemusPath( RemusApp parent, String pathinfo ) {
		this.parent = parent;
		readPath( pathinfo );
	}
	
	private void readPath(String pathinfo) {
		if ( pathinfo.compareTo("$") == 0 ) {
			input_type = DynamicInput;
		} else {

			String []pSplit = (new File(pathinfo)).getAbsolutePath().split("/");

			if ( pSplit.length > 5 ) {
				try {
					viewName = "attach";
					attachName = URLDecoder.decode( pSplit[5], "UTF-8" ) ;
				} catch (UnsupportedEncodingException e) {
				}	
			}
			if ( pSplit.length > 4 ) {
				try {
					if ( viewName == null )
						viewName = "data";
					key = URLDecoder.decode( pSplit[4], "UTF-8" ) ;
				} catch (UnsupportedEncodingException e) {
				}
			}
			if ( pSplit.length > 3 ) {
				if ( viewName == null )
					viewName = "data";
				instance = pSplit[3];
			}
			if ( pSplit.length > 2 ) {
				appletName = pSplit[2];
				String [] tmp = appletName.split("@");
				if ( tmp.length == 2 ) {
					appletName = tmp[0];
					viewName = tmp[1];
				}
			}		
			if ( pSplit.length > 1 ) {
				pipelineName = pSplit[1];	
				String [] tmp = pipelineName.split("@");
				if ( tmp.length == 2 ) {
					pipelineName = tmp[0];
					viewName = tmp[1];
					if ( pSplit.length == 3 ) {
						this.key = pSplit[2];
						this.appletName = null;
					}
				}
			}	
			if ( pipelineName != null && pipelineName.length() == 0 )
				pipelineName = null;
			url = pathinfo;
		}
	}


	public RemusPath(RemusApp parent, String inputStr, String pipelineName, String appletName) throws FileNotFoundException {
		this.parent = parent;
		if ( inputStr.compareTo("?") == 0) {
			this.appletName = appletName;
			this.pipelineName = pipelineName;
			url = "?";
			input_type=DynamicInput;
		} else {
			readPath( "/" + pipelineName + "/" + inputStr );			
		}
	}



	public int getInputType() {
		return input_type;
	}

	public String getPortPath() {
		if ( appletPortName != null )
			return "/" + pipelineName + "/" + appletName + "." + appletPortName;
		return "/" + pipelineName + "/" + appletName;
	}

	public String getViewPath() {
		if ( appletPortName != null ) {
			if ( viewName != null )
				return "/" + pipelineName + "/" + appletName + "." + appletPortName + "@" + viewName;
			return "/" + pipelineName + "/" + appletName + "." + appletPortName;
		}
		if ( viewName != null )
			return "/" + pipelineName + "/" + appletName + "@" + viewName;
		return "/" + pipelineName + "/" + appletName;
	}

	public String getInstancePath() {
		if ( appletPortName != null ) {
			if ( viewName != null )
				return "/" + pipelineName + "/" + instance + "/" + appletName + "." + appletPortName + "@" + viewName;
			return "/" + pipelineName + "/" + instance + "/"+ appletName + "." + appletPortName;
		}
		if ( viewName != null )
			return "/" + pipelineName + "/" + instance + "/" + appletName + "@" + viewName;
		return "/" + pipelineName + "/" + instance + "/" + appletName;
		
	}

	public boolean isPortName() {
		if ( appletPortName == null )
			return false;
		return true;
	}

	public String getPath() {
		return url;
	}


	public String getAppletPath() {
		return "/" + pipelineName + "/" + appletName;
	}

	public String getApplet() {
		return appletName;
	}
	
}
