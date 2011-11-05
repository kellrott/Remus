package org.remus.serverNodes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.KeyValPair;
import org.remus.core.BaseStackNode;
import org.remus.core.DataStackInfo;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.server.BaseNode;
import org.remus.thrift.AppletRef;
import org.remus.thrift.NotImplemented;

public class AppletInstanceView implements BaseNode {

	RemusPipeline pipeline;
	RemusApplet applet;
	RemusInstance inst;

	public AppletInstanceView(RemusPipeline pipeline, RemusApplet applet, RemusInstance inst) {
		this.pipeline = pipeline;
		this.applet = applet;
		this.inst = inst;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void doDelete(String name, Map params, String workerID) throws FileNotFoundException {
		throw new FileNotFoundException();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void doGet(String name, Map params, String workerID, OutputStream os) throws FileNotFoundException {

		if (params.containsKey(DataStackInfo.PARAM_FLAG)) {
			try {
				os.write(JSON.dumps(DataStackInfo.formatInfo(AppletInstanceView.class, "status", pipeline)).getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}

		String sliceStr = null;
		int sliceSize = 0;
		if (params.containsKey("slice")) {
			sliceStr = ((String []) params.get("slice"))[0];
			sliceSize = Integer.parseInt(sliceStr);
		}

		AppletRef ar = new AppletRef(pipeline.getID(), inst.toString(), applet.getID());

		if (name.length() == 0) {
			if (sliceStr == null) {
				for (String key : applet.getDataStore().listKeys(ar)) {
					try {
						os.write(JSON.dumps(key).getBytes());
						os.write("\n".getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} else {
				try { 
					for (String sliceKey : applet.getDataStore().keySlice(ar, "", sliceSize)) {
						try {
							os.write(JSON.dumps(sliceKey).getBytes());
							os.write("\n".getBytes());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} catch (TException e) {
					e.printStackTrace();
				} catch (NotImplemented e) {
					e.printStackTrace();
				}
			}
		} else {
			try {
				String [] tmp = name.split("/");
				if (tmp.length == 1) {
					String qName = URLDecoder.decode(name, "UTF-8");
					if (sliceStr == null) {
						for (Object obj : applet.getDataStore().get(ar, qName)) {
							Map out = new HashMap();
							out.put(qName, obj);
							try {
								os.write(JSON.dumps(out).getBytes());
								os.write("\n".getBytes());
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					} else {
						for (String sliceKey : applet.getDataStore().keySlice(ar, qName, sliceSize)) {
							for (Object value : applet.getDataStore().get(ar, sliceKey)) {
								Map oMap = new HashMap();
								oMap.put(sliceKey, value);
								try {
									os.write(JSON.dumps(oMap).getBytes());
									os.write("\n".getBytes());
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
					}
				} else {				
					try {
						String key = URLDecoder.decode(tmp[0], "UTF-8");
						String fileName = URLDecoder.decode(tmp[1], "UTF-8");
						
						InputStream is = applet.getAttachStore().readAttachement(ar, key, fileName);
						byte [] buffer = new byte[1024];
						int len;
						while ((len = is.read(buffer)) > 0) {
							os.write(buffer, 0, len);
						}
						is.close();		
						os.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (TException e) {
				e.printStackTrace();
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public void doPut(String name, String workerID, InputStream is, OutputStream os) throws FileNotFoundException {
		try {
			AppletRef ar = new AppletRef(pipeline.getID(), inst.toString(), applet.getID());

			if (name.length() > 0) {
				String [] tmp = name.split("/");
				if (tmp.length == 1) {
					StringBuilder sb = new StringBuilder();
					byte [] buffer = new byte[1024];
					int len;			
					while ((len = is.read(buffer)) > 0) {
						sb.append(new String(buffer, 0, len));
					}
					Object data = JSON.loads(sb.toString());
					try {
						applet.getDataStore().add(ar,
								0L, 0L,
								name, data);
					} catch (TException e) {
						e.printStackTrace();
					} catch (NotImplemented e) {
						e.printStackTrace();
					}
				} else {
					OutputStream as = applet.getAttachStore().writeAttachment(ar, tmp[0], tmp[1]);
					byte [] buffer = new byte[1024];
					int readLen;
					while ((readLen = is.read(buffer)) > 0) {
						as.write(buffer, 0, readLen);
					}
					as.close();
				}
			} else {
				StringBuilder sb = new StringBuilder();
				byte [] buffer = new byte[1024];
				int len;			
				while ((len = is.read(buffer)) > 0) {
					sb.append(new String(buffer, 0, len));
				}
				String iStr = sb.toString();
				if (iStr.length() > 0) {
					Map data = (Map) JSON.loads(iStr);
					if (data != null) {
						for (Object key : data.keySet()) {
							try {
								applet.getDataStore().add(ar,
										0L, 0L,
										(String) key, data.get(key));
							} catch (TException e) {
								e.printStackTrace();
							} catch (NotImplemented e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map postStr2Map(String iStr) throws UnsupportedEncodingException {
		Map out = new HashMap<String, String>();
		for (String el : iStr.split("&")) {
			String [] tmp = el.split("=");
			if (tmp.length > 1) {
				out.put(URLDecoder.decode(tmp[0], "UTF-8"), URLDecoder.decode(tmp[1], "UTF-8"));
			} else {
				out.put(URLDecoder.decode(el, "UTF-8"), null);
			}
		}		
		return out;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void doSubmit(String name, String workerID, InputStream is,
			OutputStream os) throws FileNotFoundException {

		AppletRef ar = new AppletRef(pipeline.getID(), inst.toString(), applet.getID());

		if (applet.getMode() == RemusApplet.STORE) {
			//A submit to an agent is translated from URL encoding to JSON and stored with a
			//UUID as the key if none is provided
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				StringBuilder sb = new StringBuilder();
				String curline;
				while ((curline = br.readLine()) != null) {
					sb.append(curline);
				}
				Map inData = postStr2Map(sb.toString());
				String key = null;
				if (name.length() > 0) {
					key = name;
				} else {
					key = (new RemusInstance()).toString();
				}
				applet.getDataStore().add(ar,
						0L, 0L,
						key, inData);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		} else if (applet.getMode() == RemusApplet.AGENT) {
							
		} else {		
			try {
				Set outSet = new HashSet<Integer>();
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				String curline = null;
				List<KeyValPair> inputList = new ArrayList<KeyValPair>();

				while ((curline = br.readLine()) != null) {
					Map inObj = (Map) JSON.loads(curline);	
					long jobID = Long.parseLong(inObj.get("id").toString());
					outSet.add((int) jobID);
					applet.getDataStore().add(ar, jobID, (Long) inObj.get("order"),
							(String) inObj.get("key"), inObj.get("value"));

				}
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotImplemented e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	@Override
	public BaseNode getChild(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	@Override
	public Iterable<String> getKeys() {
		AppletRef ar = new AppletRef(pipeline.getID(), inst.toString(), applet.getID());

		return applet.getDataStore().listKeys(ar);
	}

	@Override
	public Iterable<Object> getData(String key) {
		AppletRef ar = new AppletRef(pipeline.getID(), inst.toString(), applet.getID());
		try {
			return applet.getDataStore().get(ar, key);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotImplemented e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	 */
	
	
}
