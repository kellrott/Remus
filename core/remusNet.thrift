
namespace cpp remusNet
namespace java org.remusNet
namespace php remusNet
namespace perl remusNet
namespace py remusNet

struct StackRef {
	1: required string server,
	2: required string pipeline,
	3: required string instance,
	4: required string applet,
	5: optional list<string> keys
}

struct InstanceRef {
	1: required string server,
	2: required string pipeline,
	3: required string instance
}

struct AppletRef {
	1: required string server,
	2: required string pipeline,
	3: required string instance,
	4: required string applet
}

enum WorkMode {
	SPLIT,
	MAP,
	REDUCE,
	PIPE,
	MATCH,
	MERGE
}

enum WorkStatus {
	QUEUED,
	WORKING,
	DONE,
	ERROR
}

struct WorkDesc {
	1: required string lang,
	2: required WorkMode mode,
	3: required StackRef input,
	4: required StackRef output
}

struct KeyValPair {
	1:required string key,
	2:required string data,
	3:required i64 emitID,	
	4:required i64 jobID
}

service RemusDB {
	
	bool containKey( 1:AppletRef stack, 2:string key ),
	list<string> keySlice( 1:AppletRef stack, 2:string keyStart, 3:i32 count ),
	list<string> getValue( 1:AppletRef stack, 2:string key ),

	i64 keyCount( 1:AppletRef stack ),

	void add( 1:AppletRef stack, 2:i64 jobID, 3:i64 emitID, 4:string key, 5:string data),

	list<KeyValPair> keyValSlice( 1:AppletRef stack, 2:string startKey, 3:i32 count),

	void deleteStack( 1:AppletRef stack ),
	void deleteValue( 1:AppletRef stack, 2:string key ),
	
	i64 getTimeStamp( 1:AppletRef stack )
}

service RemusWorker {
	string startJob( 1:StackRef input, 2:WorkDesc work ),
	WorkStatus workStatus( 1:string jobID )
}

service RemusWorkServer {
	void emitWork( 1:AppletRef applet, 2:i64 jobID, 3:i64 emitID, 4:string key 5:string data ),
	void errorWork( 1:AppletRef applet, 2:i64 jobID, 3:string message)
}