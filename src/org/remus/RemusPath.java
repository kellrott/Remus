package org.remus;

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
	private String appletView = null;
	private String appletName = null;
	private String appletPortName = null;
	private String instance = null;
	private String key = null;
	private String attachName = null;
	private int input_type;

	public static final int AppletInput = 0;
	public static final int DynamicInput = 1;
	public static final int AttachInput = 2;
	public static final int StaticInput = 2;

	//static final Pattern appletSub = Pattern.compile("(\\:\\w+)\\.(\\w+)$");
	//static final Pattern pipelineAttachment = Pattern.compile("^/([^/]*)/(.*)$");
	//static final Pattern instancePat = Pattern.compile("^([^/]*)/([^/]*)$");
	//static final Pattern instanceKeyPat = Pattern.compile("^([^/]*)/([^/]*)/(.*)$");


	public RemusPath(RemusPath ref, RemusInstance instance) {
		this.parent = ref.parent;
		this.instance = instance.toString();
		if ( ref.getInputType() == DynamicInput ) {
			String submitKey = null;
			MPStore ds = ref.parent.getApplet( ref.getAppletPath() ).getDataStore();
			for ( Object path : ds.get( ref.getAppletPath() + "@instance", RemusInstance.STATIC_INSTANCE_STR, instance.toString() ) ) {
				submitKey = (String)path;
			}
			if ( submitKey != null ) {
				String submitPath = null;				
				for ( Object submitObj : ds.get( "/" + ref.getPipeline() + "@submit", RemusInstance.STATIC_INSTANCE_STR, submitKey ) ) {
					submitPath = (String)((Map)submitObj).get(Submission.InputField);
				}				
				ref = new RemusPath(ref.parent, submitPath);
			}
			this.instance = ref.instance;
		}
		this.pipelineName = ref.pipelineName;
		this.appletName = ref.appletName;
		this.appletPortName = ref.appletPortName;
		this.appletView = ref.appletView;
		this.input_type = ref.input_type;
		this.key = ref.key;
		this.url = getInstancePath(); 
	}

	public RemusPath( RemusApp parent, String pathinfo ) {
		this.parent = parent;

		String []pSplit = pathinfo.split("/");
		
		if ( pSplit != null || pSplit.length == 1) {
		}
		
		if ( pSplit.length > 1 ) {
			pipelineName = pSplit[1];	
			String [] tmp = pipelineName.split("@");
			if ( tmp.length == 2 ) {
				pipelineName = tmp[0];
				appletView = tmp[1];
			}
		}
		if ( pSplit.length > 2 ) {
			appletName = pSplit[2];
		}
		if ( pSplit.length > 3 ) {
			instance = pSplit[3];
		}
		if ( pSplit.length > 4 ) {
			try {
				key = URLDecoder.decode( pSplit[4], "UTF-8" ) ;
			} catch (UnsupportedEncodingException e) {
			}
		}
		if ( pSplit.length > 5 ) {
			try {
				attachName = URLDecoder.decode( pSplit[5], "UTF-8" ) ;
			} catch (UnsupportedEncodingException e) {
			}	
		}
		if ( pipelineName != null && pipelineName.length() == 0 )
			pipelineName = null;
		url = pathinfo;
	}

	

	public RemusPath(RemusApp parent, String inputStr, String pipelineName, String appletName) throws FileNotFoundException {
		this.parent = parent;
		if ( inputStr.compareTo("?") == 0) {
			this.appletName = appletName;
			this.pipelineName = pipelineName;
			url = "?";
			input_type=DynamicInput;
		} else if ( inputStr.compareTo("$") == 0) {
			this.appletName = appletName;
			this.pipelineName = pipelineName;
			url = "$";
			input_type=StaticInput;
		} else {
			url = "/" + pipelineName + "/" + inputStr; 
		}
	}
	
	public long getKeyCount( MPStore ds, int maxCount ) {
		if ( key != null )
			return 1;
		return ds.keyCount( getViewPath(), instance, maxCount );			
	}

	public Iterable<String> listKeys( MPStore ds ) {
		if ( key != null ) {
			return Arrays.asList(key);
		}
		return ds.listKeys(getViewPath(), instance);
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
		if ( appletPortName != null )
			return "/" + pipelineName + "/" + appletName + "." + appletPortName + "@" + appletView;
		return "/" + pipelineName + "/" + appletName + "@" + appletView;
	}

	public String getInstancePath() {
		return getViewPath() + "/" + instance;
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

	public String getApplet() {
		return appletName;
	}

	public String getPipeline() {
		return pipelineName;
	}

	public String getAttachment() {
		// TODO Auto-generated method stub
		return attachName;
	}


}
