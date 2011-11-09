package org.remus.mapred;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.remus.JSON;
import org.remus.RemusAttach;
import org.remus.RemusDB;
import org.remus.RemusDatabaseException;
import org.remus.core.AppletInstance;
import org.remus.core.PipelineSubmission;
import org.remus.core.RemusApp;
import org.remus.core.RemusInstance;
import org.remus.core.RemusPipeline;
import org.remus.thrift.AppletRef;
import org.remus.thrift.Constants;
import org.remus.thrift.JobState;
import org.remus.thrift.JobStatus;
import org.remus.thrift.NotImplemented;
import org.remus.thrift.RemusNet;
import org.remus.thrift.WorkDesc;
import org.remus.thrift.WorkMode;
import org.remus.RemusDBSliceIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkEngine implements Runnable {
	private WorkDesc work;
	private RemusDB db;
	private MapReduceFunction mapred;
	private JobState status;
	private String message;
	private RemusAttach attach;
	private Logger logger;
	private boolean canceled = false; 
	public WorkEngine(WorkDesc work, RemusNet.Iface db, RemusNet.Iface attach, MapReduceFunction mapred) {
		this.work = work;
		this.db = RemusDB.wrap(db);
		this.attach = RemusAttach.wrap(attach);
		this.mapred = mapred;
		status = JobState.QUEUED;
		logger = LoggerFactory.getLogger(WorkEngine.class);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void run() {
		AppletRef arWork = new AppletRef(work.workStack.pipeline, 
				work.workStack.instance, work.workStack.applet + Constants.WORK_APPLET);
		status = JobState.WORKING;
		message = null;
		Map stackInfo = (Map) JSON.loads(work.infoJSON);

		AppletRef outRef = new AppletRef(
				work.workStack.pipeline,
				work.workStack.instance,
				work.workStack.applet
		);


		PipelineSubmission sub = new PipelineSubmission(stackInfo);
		try { 
			mapred.init(stackInfo);
			if (work.mode == WorkMode.MAP) {
				String inputInstStr = sub.getInputInstance();				
				RemusInstance inst = RemusInstance.getInstance(db, 
						work.workStack.pipeline, 
						inputInstStr);
				AppletRef ar = new AppletRef(
						work.workStack.pipeline,
						inst.toString(),
						sub.getInputApplet()
				);

				for (long jobID = work.workStart; jobID < work.workEnd; jobID++) {
					int keyCount = 0;
					for (Object key : db.get(arWork, Long.toString(jobID))) {
						MapReduceCallback cb = new MapReduceCallback(work.workStack.pipeline,
								work.workStack.instance, work.workStack.applet,
								new PipelineSubmission(stackInfo), db, attach);
						for (Object value : db.get(ar, (String) key)) {
							mapred.map((String) key, value, cb);
						}
						if (!canceled) {
							cb.writeEmits(outRef, jobID);
							logger.debug(work.workStack.instance + ":" + work.workStack.applet + ":" + jobID + " EmitTotal: " + cb.emitCount);
						}
						keyCount+=1;
					}
					if (keyCount == 0) {
						logger.error("WORK KEY not found:" + jobID);
						throw new Exception("Work Key not found");
					}
				}
				status = JobState.DONE;
			} else if (work.mode == WorkMode.SPLIT) {
				MapReduceCallback cb = new MapReduceCallback(work.workStack.pipeline,
						work.workStack.instance, work.workStack.applet,
						new PipelineSubmission(stackInfo), db, attach);

				if (!canceled) {
					mapred.split(stackInfo, cb);
					cb.writeEmits(outRef, 0);
					logger.info("Emiting: " + cb.emitCount);
				}
				status = JobState.DONE;
			} else if (work.mode == WorkMode.REDUCE) {
				String inputInstStr = sub.getInputInstance();				
				RemusInstance inst = RemusInstance.getInstance(db, 
						work.workStack.pipeline, 
						inputInstStr);
				AppletRef ar = new AppletRef(
						work.workStack.pipeline,
						inst.toString(),
						sub.getInputApplet()
				);

				for (long jobID = work.workStart; jobID < work.workEnd; jobID++) {
					MapReduceCallback cb = new MapReduceCallback(work.workStack.pipeline,
							work.workStack.instance,
							work.workStack.applet,
							new PipelineSubmission(stackInfo),
							db, attach);
					for (Object key : db.get(arWork, Long.toString(jobID))) {
						mapred.reduce((String) key, db.get(ar, (String) key), cb);
					}
					if (!canceled) {
						cb.writeEmits(outRef, jobID);
						logger.info("Emiting: " + cb.emitCount);
					}
				}
				status = JobState.DONE;
			} else if (work.mode == WorkMode.MATCH) {

				String leftInputInstStr = sub.getLeftInputInstance();				
				String rightInputInstStr = sub.getRightInputInstance();

				RemusInstance leftInst = RemusInstance.getInstance(db, 
						work.workStack.pipeline, 
						leftInputInstStr);
				RemusInstance rightInst = RemusInstance.getInstance(db, 
						work.workStack.pipeline, 
						rightInputInstStr);

				AppletRef leftAr = new AppletRef(
						work.workStack.pipeline,
						leftInst.toString(),
						sub.getLeftInputApplet()
				);
				AppletRef rightAr = new AppletRef(
						work.workStack.pipeline,
						rightInst.toString(),
						sub.getRightInputApplet()
				);

				for (long jobID = work.workStart; jobID < work.workEnd; jobID++) {
					MapReduceCallback cb = new MapReduceCallback(work.workStack.pipeline, work.workStack.instance,
							work.workStack.applet, new PipelineSubmission(stackInfo), db, attach);
					for (Object key : db.get(arWork, Long.toString(jobID))) {
						mapred.match((String) key, db.get(leftAr, (String) key), db.get(rightAr, (String) key), cb);
					}
					if (!canceled) {
						cb.writeEmits(outRef, jobID);
						logger.info("Emiting: " + cb.emitCount);
					}
				}
				status = JobState.DONE;

			} else if (work.mode == WorkMode.MERGE) {
				String leftInputInstStr = sub.getLeftInputInstance();				
				String rightInputInstStr = sub.getRightInputInstance();

				RemusInstance leftInst = RemusInstance.getInstance(db, 
						work.workStack.pipeline, 
						leftInputInstStr);
				RemusInstance rightInst = RemusInstance.getInstance(db, 
						work.workStack.pipeline, 
						rightInputInstStr);

				AppletRef leftAr = new AppletRef(
						work.workStack.pipeline,
						leftInst.toString(),
						sub.getLeftInputApplet()
				);
				AppletRef rightAr = new AppletRef(
						work.workStack.pipeline,
						rightInst.toString(),
						sub.getRightInputApplet()
				);

				for (long jobID = work.workStart; jobID < work.workEnd; jobID++) {
					MapReduceCallback cb = new MapReduceCallback(
							work.workStack.pipeline, 
							work.workStack.instance,
							work.workStack.applet,
							new PipelineSubmission(stackInfo),
							db, attach);
					if (sub.getAxis() == PipelineSubmission.LEFT_AXIS) {
						for (Object leftKey : db.get(arWork, Long.toString(jobID))) {
							for (String rightKey : db.listKeys(rightAr)) {
								mapred.merge(
										(String) leftKey, db.get(leftAr, (String) leftKey), 
										(String) rightKey, db.get(rightAr, (String) rightKey), 
										cb);
							}
						}
					} else {
						for (Object rightKey : db.get(arWork, Long.toString(jobID))) {
							for (String leftKey : db.listKeys(leftAr)) {
								mapred.merge(
										(String) leftKey, db.get(leftAr, (String) leftKey), 
										(String) rightKey, db.get(rightAr, (String) rightKey), 
										cb);
							}
						}
					}
					if (!canceled) {
						cb.writeEmits(outRef, jobID);
						logger.info("Emiting: " + cb.emitCount);
					}
				}
				status = JobState.DONE;
			} else if (work.mode == WorkMode.PIPE) {
				List inputList = sub.getInputList();
				List sourceList = sub.getSourceList();
				Map<String,Object> siList = new HashMap<String,Object>();
				int i = 0;
				for (Object inputInfo : inputList) {
					RemusInstance inst = RemusInstance.getInstance(db, 
							work.workStack.pipeline, 
							(String) ((Map) inputInfo).get("_instance"));
					AppletRef ar = new AppletRef(
							work.workStack.pipeline,
							inst.toString(),
							(String) ((Map) inputInfo).get("_applet")
					);
					//siList.put((String) siList.get(i), new StackIterator(db, ar));
					
					siList.put((String) sourceList.get(i), new StackInterface(db, attach, ar));
					i++;
				}

				MapReduceCallback cb = new MapReduceCallback(work.workStack.pipeline,
						work.workStack.instance,
						work.workStack.applet,
						new PipelineSubmission(stackInfo),
						db, attach);
				mapred.pipe(siList, cb);
				if (!canceled) {
					cb.writeEmits(outRef, 0);
					logger.info("Emiting: " + cb.emitCount);
				}
				status = JobState.DONE;
			} else if (work.mode == WorkMode.REMAP) {
				List inputList = sub.getInputList();
				Object remapInfo = null;
				AppletRef remapAR = null;
				class MappedInfo {
					Object info;
					RemusInstance inst;
					AppletRef applet;
				};

				List<MappedInfo> mappedList = new LinkedList<MappedInfo>();
				for (Object inputInfo : inputList) {
					if (remapInfo == null) {
						remapInfo = inputInfo;
						RemusInstance remapInst = RemusInstance.getInstance(db, 
								work.workStack.pipeline, 
								(String) ((Map) remapInfo).get("_instance"));

						remapAR = new AppletRef(
								work.workStack.pipeline,
								remapInst.toString(),
								(String) ((Map) inputInfo).get("_applet")
						);
					} else {
						MappedInfo m = new MappedInfo();
						m.info = inputInfo;
						m.inst = RemusInstance.getInstance(db, 
								work.workStack.pipeline, 
								(String) ((Map) inputInfo).get("_instance"));
						m.applet =  new AppletRef(
								work.workStack.pipeline,
								m.inst.toString(),
								(String) ((Map) inputInfo).get("_applet")
						);
						mappedList.add(m);				
					}
				}

				for (long jobID = work.workStart; jobID < work.workEnd; jobID++) {
					MapReduceCallback cb = new MapReduceCallback(
							work.workStack.pipeline, 
							work.workStack.instance,
							work.workStack.applet,
							new PipelineSubmission(stackInfo),
							db, attach);
					class RemapCallStack {
						private RemusDB db;
						private MapReduceCallback cb;
						private List<MappedInfo> mappedList;
						private List<String> mappedKeySet;
						private String key;

						public RemapCallStack(RemusDB db, MapReduceCallback cb, List<MappedInfo> mappedList) {
							this.db = db;
							this.cb = cb;
							this.mappedList = mappedList;
						}

						public void setKeys(String key, List<String> mappedKeySet) {
							this.key = key;
							this.mappedKeySet = mappedKeySet;
						}

						void remapIter() throws NotSupported, Exception {
							remapIter(0, new LinkedList());
						}

						void remapIter(int level, List<Object> valueStack) throws NotSupported, Exception {
							if (level >= mappedKeySet.size()) {
								logger.info("CALLING:" + key + " " + valueStack);
								mapred.map((String) key, valueStack, cb);								
							} else {
								LinkedList newStack = new LinkedList(valueStack);
								for (Object value : db.get(mappedList.get(level).applet, mappedKeySet.get(level))) {
									newStack.add(value);
									remapIter(level + 1, newStack);
									newStack.removeLast();
								}
							}
						}
					}
					RemapCallStack stack = new RemapCallStack(db, cb, mappedList);
					stack.db = db;
					for (Object key : db.get(arWork, Long.toString(jobID))) {
						for (Object value : db.get(remapAR, (String) key)) {
							List remapKeys = (List) value;
							List<String> mappedKeySets = new ArrayList<String>(remapKeys.size());
							for (Object rKey : remapKeys) {
								mappedKeySets.add(rKey.toString());
							}
							stack.setKeys(key.toString(), mappedKeySets);							
							stack.remapIter();
						}
					}

					if (!canceled) {
						cb.writeEmits(outRef, jobID);
						logger.info(work.workStack.instance + ":" + work.workStack.applet + ":" + jobID + " EmitTotal: " + cb.emitCount);
					}
				}
				status = JobState.DONE;
			} else if (work.mode == WorkMode.REREDUCE) {
				List inputList = sub.getInputList();
				Object remapInfo = null;
				AppletRef remapAR = null;
				class MappedInfo {
					Object info;
					RemusInstance inst;
					AppletRef applet;
				};

				List<MappedInfo> mappedList = new LinkedList<MappedInfo>();
				for (Object inputInfo : inputList) {
					if (remapInfo == null) {
						remapInfo = inputInfo;
						RemusInstance remapInst = RemusInstance.getInstance(db, 
								work.workStack.pipeline, 
								(String) ((Map) remapInfo).get("_instance"));

						remapAR = new AppletRef(
								work.workStack.pipeline,
								remapInst.toString(),
								(String) ((Map) inputInfo).get("_applet")
						);
					} else {
						MappedInfo m = new MappedInfo();
						m.info = inputInfo;
						m.inst = RemusInstance.getInstance(db, 
								work.workStack.pipeline, 
								(String) ((Map) inputInfo).get("_instance"));
						m.applet =  new AppletRef(
								work.workStack.pipeline,
								m.inst.toString(),
								(String) ((Map) inputInfo).get("_applet")
						);
						mappedList.add(m);				
					}
				}

				for (long jobID = work.workStart; jobID < work.workEnd; jobID++) {
					MapReduceCallback cb = new MapReduceCallback(
							work.workStack.pipeline, 
							work.workStack.instance,
							work.workStack.applet,
							new PipelineSubmission(stackInfo),
							db, attach);
					for (Object key : db.get(arWork, Long.toString(jobID))) {
						for (Object value : db.get(remapAR, (String) key)) {
							List remapKeys = (List) value;
							List remapValues = new LinkedList();	
							int i = 0;
							for (Object rKey : remapKeys) {
								remapValues.add(db.get(mappedList.get(i).applet, rKey.toString()));
								i++;
							}
							mapred.reduce(key.toString(), remapValues, cb);
						}
					}
					if (!canceled) {
						cb.writeEmits(outRef, jobID);
						logger.info(work.workStack.instance + ":" + work.workStack.applet + ":" + jobID + " EmitTotal: " + cb.emitCount);
					}
				}
				status = JobState.DONE;
			} else {
				status = JobState.ERROR;
			}
		} catch (Exception e) {
			status = JobState.ERROR;
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			logger.info(sw.toString());
			message = sw.toString();
		}
		cleanup();
	}

	public JobStatus getStatus() {
		JobStatus out = new JobStatus();
		out.status = status;
		out.errorMsg = message;
		return out;
	}

	public void cancel() {
		canceled = true;
		if (status != JobState.WORKING) {
			cleanup();
		}
	}

	public void cleanup() {
		mapred.cleanup();
	}

	
	public class StackInterface implements Iterable {
		private RemusApp app;
		private RemusPipeline pipe;
		private AppletInstance ai;
		private RemusDB db;
		private RemusAttach attach;
		private AppletRef stack;
		public StackInterface(RemusDB db, RemusAttach attach, AppletRef stack) throws RemusDatabaseException {
			this.db = db;
			this.attach = attach;
			this.stack = stack;
			app = new RemusApp(db, attach);
			pipe = app.getPipeline(stack.pipeline);
			ai = pipe.getAppletInstance(new RemusInstance(stack.instance), stack.applet);
		}
		
		public Map get_info() throws TException, NotImplemented {
			return ai.getInstanceInfo().getMap();
		}		
		
		@Override
		public Iterator iterator() {
			return new StackIterator(db, stack);
		}	
		
	}
	
	public class StackIterator extends RemusDBSliceIterator<Object []> {
		public StackIterator(RemusDB db, AppletRef stack) {
			super(db, stack, "", "", true);
		}
		@Override
		public void processKeyValue(String key, Object val, long jobID, long emitID) {
			addElement(new Object []  {key, val});
		}
	}

	@Override
	public String toString() {
		return work.workStack.instance + ":" + work.workStack.applet;
	}

}
