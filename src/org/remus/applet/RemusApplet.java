package org.remus.applet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.mpstore.KeyValuePair;
import org.mpstore.MPStore;
import org.remus.CodeFragment;
import org.remus.InputReference;
import org.remus.RemusInstance;
import org.remus.RemusPipeline;
import org.remus.WorkDescription;


public class RemusApplet {

	public static RemusApplet newApplet( String path, CodeFragment code, int type ) {
		RemusApplet out = new RemusApplet();

		switch (type) {
		case MAPPER: {
			out.workGenerator = MapGenerator.class; //new MapperApplet();	
			break;
		}
		case REDUCER: {
			out.workGenerator = ReduceGenerator.class;	
			break;
		}
		case SPLITTER: {
			out.workGenerator = SplitGenerator.class;	
			break;
		}
		case MERGER: {
			out.workGenerator = MergeGenerator.class;	
			break;
		}
		case MATCHER: {
			out.workGenerator = MatchGenerator.class;	
			break;
		}
		case PIPE: {
			out.workGenerator = PipeGenerator.class;	
			break;
		}
		}
		if ( out != null ) {
			out.code = code;
			out.path = path;
			out.type = type;
			out.inputs = null;
			out.workCache = new HashMap<RemusInstance, HashMap<Long,WorkDescription>>();
			out.activeInstances = new LinkedList<RemusInstance>();			
		}
		return out;
	}

	public static final int MAPPER = 1;
	public static final int MERGER = 2;
	public static final int MATCHER = 3;
	public static final int SPLITTER = 4;
	public static final int REDUCER = 5;
	public static final int PIPE = 6;

	Class workGenerator;
	String path;
	List<InputReference> inputs = null, lInputs = null, rInputs = null;
	List<String> outputs = null;
	CodeFragment code;
	MPStore datastore;
	int type;
	protected RemusPipeline pipeline = null;
	HashMap<RemusInstance,HashMap<Long,WorkDescription>> workCache;
	LinkedList<RemusInstance> activeInstances;

	public void addInput( InputReference in ) {
		if ( inputs == null )
			inputs = new ArrayList<InputReference>();
		inputs.add(in);
	}	

	public void addLeftInput( InputReference in ) {
		if ( lInputs == null )
			lInputs = new LinkedList<InputReference>();
		lInputs.add(in);
		addInput(in);
	}

	public void addRightInput( InputReference in ) {
		if ( rInputs == null )
			rInputs = new LinkedList<InputReference>();
		rInputs.add(in);
		addInput(in);
	}

	public void addOutput( String name ) {
		if ( outputs == null ) 
			outputs = new LinkedList<String>();
		outputs.add(name);
	}

	public CodeFragment getCode() {
		return code;
	}

	public List<InputReference> getInputs() {
		if ( inputs != null )
			return inputs;
		return new ArrayList<InputReference>();
	}

	public String [] getOutputs() {
		if ( outputs != null  )
			return outputs.toArray( new String[0] );
		return new String[0];
	}

	public String getPath() {
		return path;
	}

	public String getSource() {
		return code.getSource();
	}

	public Map getInfo() {
		Map out = new HashMap();
		if ( type==MAPPER ) {
			out.put("mode", "map");
		}
		if ( type==REDUCER ) {
			out.put("mode", "reduce");
		}
		if(type==SPLITTER) {
			out.put("mode", "split");
		}
		if(type==PIPE) {
			out.put("mode", "pipe");
		}
		if(type==MERGER) {
			out.put("mode", "merge");
		}
		if(type==MATCHER) {
			out.put("mode", "match");
		}
		if ( outputs != null && outputs.size() > 0 ) {
			out.put("output", outputs);
		}
		return out;
	}

	public void setPipeline(RemusPipeline remusPipeline) {
		this.pipeline = remusPipeline;		
		this.datastore = remusPipeline.getCodeManager().getApp().getDataStore();
	}

	public RemusPipeline getPipeline() {
		return this.pipeline;
	}


	public boolean isReady( RemusInstance remusInstance ) {
		if ( hasInputs() ) {
			boolean allReady = true;
			for ( InputReference iRef : inputs ) {
				if ( iRef.getInputType() == InputReference.AppletInput ) {
					RemusApplet iApplet = getPipeline().getApplet( iRef.getAppletPath() );
					if ( iApplet != null ) {
						if ( !iApplet.isComplete(remusInstance) ) {
							allReady = false;
						}
					} else {
						allReady = false;
					}
				} else if ( iRef.getInputType() == InputReference.DynamicInput ) {
					if ( datastore.get( "/@submit", RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) == null ) {
						allReady = false;
					}
				} else if (  iRef.getInputType() == InputReference.ExternalInput ) {
				} else if (  iRef.getInputType() == InputReference.StaticInput ) {
				} else {				
					allReady = false;
				}
			}
			return allReady;
		}		
		return true;
	}


	public boolean isComplete( RemusInstance remusInstance ) {
		String pathStr = getPath();
		boolean found = false;
		for ( Object instStr : datastore.get( getPath() + "@done", RemusInstance.STATIC_INSTANCE_STR, remusInstance.toString() ) ) {
			if ( pathStr.compareTo((String)instStr) == 0 ) {
				found = true;
			}
		}
		return found;
	}

	public void setComplete(RemusInstance remusInstance) {
		datastore.add( getPath() + "@done", RemusInstance.STATIC_INSTANCE_STR, 0, 0, remusInstance.toString(), getPath());
	}

	public boolean hasInputs() {
		if ( inputs == null )
			return false;
		return true;
	}

	public int getType() {
		return type;
	}

	public WorkDescription getWork(RemusInstance inst, int jobID) {
		WorkDescription out = null;
		for ( Object obj : datastore.get(getPath() + "@work", inst.toString(), Long.toString(jobID) ) ) {
			out = (WorkDescription) new WorkDescription(this, inst, jobID, obj);
		}
		return out;
	}

	static int maxCacheSize = 1000;
	public Collection<WorkDescription> getWorkList(RemusInstance inst) {
		if ( !workCache.containsKey(inst)) {
			generateWorkCache(inst);
		}
		if ( workCache.containsKey(inst) ) {
			if ( workCache.get(inst).size() <= 0 ) {
				updateWorkCache(inst);
			}
			return workCache.get(inst).values();
		}
		return null;
	}	

	private void updateWorkCache(RemusInstance inst) {
		HashMap<Long,WorkDescription> a = workCache.get(inst);
		a.clear();
		boolean found = false;
		for ( KeyValuePair kv : datastore.listKeyPairs( getPath() + "@work", inst.toString() ) ) {
			if ( a.size() < maxCacheSize ) {
				long jobID = Long.parseLong(kv.getKey());				
				a.put(jobID, new WorkDescription(this, inst, jobID, kv.getValue()) );
			}
			found = true;
		}
		if (!found) {
			setComplete(inst);
		}
	}

	public void finishWork(RemusInstance remusInstance, long jobID) {
		if ( workCache.get(remusInstance).containsKey(jobID) ) {
			workCache.get(remusInstance).remove(jobID);
		}
		datastore.delete(getPath() + "@work", remusInstance.toString(), Long.toString(jobID) );
	}

	private void generateWorkCache(RemusInstance inst) {
		HashMap<Long,WorkDescription> out = new HashMap<Long,WorkDescription>();
		if ( !isComplete(inst) ) {
			if ( isReady(inst)) {
				try {
					WorkGenerator gen = (WorkGenerator) workGenerator.newInstance();
					gen.init(this);
					gen.startWork(inst);
					datastore.delete( getPath() + "@work", inst.toString() );
					WorkDescription curWork = null;					
					while ( (curWork=gen.nextWork()) != null) {
						datastore.add( getPath() + "@work", inst.toString(), 0L, 0L, Long.toString( curWork.jobID ), curWork.desc );
						if ( out.size() < maxCacheSize ) {
							out.put(curWork.jobID, curWork );
						}
					}
					workCache.put(inst, out );
				} catch (InstantiationException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IllegalAccessException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

			}
		}
	}

	public void addInstance(RemusInstance instance) {
		if ( !datastore.containsKey(getPath() + "@instances", RemusInstance.STATIC_INSTANCE_STR, instance.toString() ) ) {
			datastore.add(getPath() + "@instances", RemusInstance.STATIC_INSTANCE_STR, 0, 0, instance.toString(), "");
		}
	}

	public Collection<RemusInstance> getInstanceList() {
		Collection<RemusInstance> out = new HashSet<RemusInstance>( );
		for ( KeyValuePair kv : datastore.listKeyPairs( getPath() + "@instances", RemusInstance.STATIC_INSTANCE_STR ) ) {
			out.add( new RemusInstance( (String)kv.getKey() ) );
		}		

		for ( KeyValuePair kv : datastore.listKeyPairs( getPath() + "@done", RemusInstance.STATIC_INSTANCE_STR ) ) {
			out.add( new RemusInstance( (String)kv.getKey() ) );
		}

		for ( KeyValuePair kv : datastore.listKeyPairs( getPath() + "@submit", RemusInstance.STATIC_INSTANCE_STR ) ) {
			out.add( new RemusInstance( (String)kv.getKey() ) );
		}

		return out;
	}


}
