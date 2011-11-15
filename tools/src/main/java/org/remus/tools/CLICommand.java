package org.remus.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.KeyValPair;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.core.AppletInstance;
import org.remus.core.AppletInstanceStack;
import org.remus.core.BaseStackIterator;
import org.remus.core.BaseStackNode;
import org.remus.core.DataStackNode;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusApp;
import org.remus.core.RemusApplet;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.plugin.PeerManager;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.PeerInfoThrift;
import org.remus.thrift.RemusNet;

public class CLICommand {

	public static final int QUIT = 1;
	public static final int SHOW = 2;
	public static final int USE = 3;
	public static final int SELECT = 4;
	public static final int DROP = 5;
	public static final int DELETE = 6;
	public static final int LOAD = 7;

	public static final int SERVERS = 1;
	public static final int LIST = 2;
	public static final int PIPELINES = 3;
	public static final int STACKS = 4;
	public static final int APPLETS = 5;

	private int type;	
	private int system;
	private String pipelineName = null;
	private String field = null;
	private String stack = null;
	private String path;
	private List<Selection> selection;
	private List<Conditional> conditional;
	private Integer limit = null;

	public CLICommand(int cmdType) {
		this.type = cmdType;
	}

	public void setSystem(int system) {
		this.system = system;
	}

	public int getType() {
		return type;
	}

	public void runCommand(CLIInterface cli) throws NotImplemented, TException, RemusDatabaseException, IOException {
		switch (type) {
		case SHOW: {
			doShow(cli);
		}
		break;
		case USE: {
			cli.changePipeline(pipelineName);
		}
		break;
		case SELECT: {
			doSelect(cli);
		}
		break;
		case DELETE: {
			doDelete(cli);
		}
		break;		
		case DROP: {
			doDrop(cli);
		}
		break;
		case LOAD: {
			doLoad(cli);
		}
		break;
		}
	}

	private void doLoad(CLIInterface cli) throws IOException {
		cli.println("LOADING: " + path);
	}

	private void doDrop(CLIInterface cli) throws RemusDatabaseException, TException {
		RemusApp app = cli.getRemusApp();
		if (pipelineName != null) {
			RemusPipeline pipe = app.getPipeline(pipelineName);
			app.deletePipeline(pipe);
			cli.changePipeline(null);
		}
	}

	private void doSelect(final CLIInterface cli) throws RemusDatabaseException, TException, NotImplemented, IOException {
		BaseStackNode curStack = null;

		if (stack.compareTo("@instance") == 0) {
			RemusDB db = cli.getDataSource();
			AppletRef ar = new AppletRef(cli.getPipeline().getID(), Constants.STATIC_INSTANCE, Constants.INSTANCE_APPLET);
			curStack = new DataStackNode(db, ar);
		} else if (stack.compareTo("@work") == 0) {
			RemusDB db = cli.getDataSource();
			AppletRef ar = new AppletRef(cli.getPipeline().getID(), Constants.STATIC_INSTANCE, Constants.WORK_APPLET);
			curStack = new DataStackNode(db, ar);			
		} else if (stack.compareTo("@workstat") == 0) {
			RemusNet.Iface manager = cli.getManager();
			AppletRef ar = new AppletRef(cli.getPipeline().getID(), Constants.STATIC_INSTANCE, Constants.WORKSTAT_APPLET);
			curStack = new DataStackNode(manager, ar);			
		} else {
			String [] tmp = stack.split(":");
			RemusPipeline pipeline = cli.getPipeline();
			RemusDB db = cli.getDataSource();
			RemusAttach attach = cli.getAttachStore();
			if (tmp.length == 2) {
				RemusApplet applet = pipeline.getApplet(tmp[1]);
				AppletInstance ai = applet.getAppletInstance(tmp[0]);
				AppletRef ar = ai.getAppletRef();
				curStack = new DataStackNode(db, ar);
			} else if (tmp.length == 3) {
				RemusApplet applet = pipeline.getApplet(tmp[1] + ":" + tmp[2]);
				AppletInstance ai = applet.getAppletInstance(tmp[0]);
				AppletRef ar = ai.getAppletRef();
				curStack = new DataStackNode(db, ar);
			} else {
				curStack = new AppletInstanceStack(db, attach, cli.getPipeline().getID()) {
					@Override
					public void add(String key, String data) {
						// TODO Auto-generated method stub

					}
				};
			}
		}
		if (curStack != null) {
			BaseStackIterator<Object> iter = new BaseStackIterator<Object>(curStack, "", "", true) {
				private long counter = 0;
				@Override
				public void processKeyValue(String key, Object val) {
					boolean select = true;
					if (limit != null && counter >= limit) {
						stop();
						return;
					} else if (conditional != null) {
						Conditional c = conditional.get(0);						
						if (!c.evaluate(key, val)) {
							select = false;
						}						
					}
					if (select) {
						StringBuilder o = new StringBuilder();
						boolean first = true;
						for (Selection sel : selection) {
							if (!first) {
								o.append("|");
							}
							o.append(sel.getString(key, val));
						}
						try {
							cli.println(o.toString());
							counter++;
						} catch (IOException e) {
							// TODO Auto-generated catch block
							//e.printStackTrace();
						}
					}
					addElement(null);					
				}
			};

			while (iter.hasNext()) {
				iter.next();
			}
		}
	}

	private void doDelete(final CLIInterface cli) throws RemusDatabaseException, TException, NotImplemented, IOException {
		String [] tmp = stack.split(":");
		RemusPipeline pipeline = cli.getPipeline();
		BaseStackNode curStack = null;
		RemusDB db = cli.getDataSource();
		RemusAttach attach = cli.getAttachStore();
		if (tmp.length == 2) {
			RemusApplet applet = pipeline.getApplet(tmp[1]);
			AppletInstance ai = applet.getAppletInstance(tmp[0]);
			AppletRef ar = ai.getAppletRef();
			curStack = new DataStackNode(db, ar);
		} else if (tmp.length == 3) {
			RemusApplet applet = pipeline.getApplet(tmp[1] + ":" + tmp[2]);
			AppletInstance ai = applet.getAppletInstance(tmp[0]);
			AppletRef ar = ai.getAppletRef();
			curStack = new DataStackNode(db, ar);
		} else {
			curStack = new AppletInstanceStack(db, attach, cli.getPipeline().getID()) {
				@Override
				public void add(String key, String data) {
				}
			};
		}
		final BaseStackNode fCurStack = curStack;
		if (curStack != null) {
			BaseStackIterator<Object> iter = new BaseStackIterator<Object>(curStack, "", "", true) {
				private long counter = 0;
				@Override
				public void processKeyValue(String key, Object val) {
					boolean select = true;
					if (limit != null && counter >= limit) {
						stop();
						return;
					} else if (conditional != null) {
						Conditional c = conditional.get(0);						
						if (!c.evaluate(key, val)) {
							select = false;
						}						
					}
					if (select) {						
						fCurStack.delete(key);						
						try {
							cli.println("DELETE: " + key);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						counter++;

					}
					addElement(null);					
				}
			};

			while (iter.hasNext()) {
				iter.next();
			}
		}
	}

	private void doShow(CLIInterface cli) throws NotImplemented, TException, IOException, RemusDatabaseException {
		switch (system) {
		//case SERVERS: {
		//	for (PeerInfoThrift info : pm.getPeers()) {
		//		cli.println(info.name + "\t" + info.addr.host + ":" + info.addr.port + "\t" + info.peerID);				
		//	}
		//}
		//break;
		case PIPELINES:{
			RemusApp app = cli.getRemusApp();
			for (String name : app.getPipelines()) {
				cli.println(name);
			}
		}
		break;
		case STACKS: {
			RemusPipeline pipe = cli.getPipeline();
			if (pipe != null) {
				Map<RemusInstance, String> subMap = new HashMap<RemusInstance, String>();
				for (KeyValPair kv : pipe.getSubmitValues()) {
					PipelineSubmission ps = new PipelineSubmission(kv.getValue());
					subMap.put(ps.getInstance(), kv.getKey());
				}
				for (String appletName : pipe.getMembers()) {
					RemusApplet applet = pipe.getApplet(appletName);
					for (RemusInstance inst : applet.getInstanceList()) {
						if (subMap.containsKey(inst)) {
							cli.println(subMap.get(inst) + ":" + appletName);
						} else {
							cli.println(inst.toString() + ":" + appletName);
						}
					}
				}
			}
		}
		break;
		case APPLETS: {
			RemusPipeline pipe = cli.getPipeline();
			if (pipe != null) {
				for (String member : pipe.getMembers()) {
					cli.println(member);
				}
			}
		}
		break;
		}			
	}

	public void setPipeline(String pn) {
		pipelineName = pn;
	}

	public void setField(String f) {
		field = f;		
	}

	public void setStack(String s) {
		stack = s;		
	}

	public void setPath(String path) {
		this.path = path;		
	}

	public void setSelection(List<Selection> f) {
		selection = f;
	}

	public void setConditional(List<Conditional> c) {
		conditional = c;
	}

	public void setLimit(int limit) {
		this.limit  = limit;
	}

}
