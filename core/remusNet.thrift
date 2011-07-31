
namespace cpp remusNet
namespace java org.remusNet.thrift
namespace php remusNet
namespace perl remusNet
namespace py remusNet

struct StackRef {
	1: required string pipeline,
	2: required string instance,
	3: required string applet,
	4: optional list<string> keys
}

struct InstanceRef {
	1: required string pipeline,
	2: required string instance
}

struct AppletRef {
	1: required string pipeline,
	2: required string instance,
	3: required string applet
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

struct KeyValJSONPair {
	1:required string key,
	2:required string valueJson,
	3:required i64 jobID,
	4:required i64 emitID
}

service RemusDBThrift {
	
	bool containsKey( 1:AppletRef stack, 2:string key ),
	list<string> keySlice( 1:AppletRef stack, 2:string keyStart, 3:i32 count ),
	list<string> getValueJSON( 1:AppletRef stack, 2:string key ),

	i64 keyCount( 1:AppletRef stack, 2:i32 maxCount ),

	void addData( 1:AppletRef stack, 2:i64 jobID, 3:i64 emitID, 4:string key, 5:string data),

	list<KeyValJSONPair> keyValJSONSlice( 1:AppletRef stack, 2:string startKey, 3:i32 count),

	void deleteStack( 1:AppletRef stack ),
	void deleteValue( 1:AppletRef stack, 2:string key ),
	
	i64 getTimeStamp( 1:AppletRef stack )
}

service RemusAttachThrift {

	void initAttachment( 1:AppletRef stack, 2:string key, 3:string name, 4:i64 length),
	
	binary readBlock( 1:AppletRef stack, 2:string key, 3:string name, 4:i64 offset, 5:i32 length ),
	
	void writeBlock( 1:AppletRef stack, 2:string key, 3:string name, 4:i64 offset, 5:binary data ),
	
	list<string> listAttachments( 1:AppletRef stack, 2:string key ),

	void deleteAttachment( 1:AppletRef stack, 2:string key, 3:string name ),	

	bool hasAttachment( 1:AppletRef stack, 2:string key, 3:string name)
	
}

service RemusWorker {
	string startJob( 1:StackRef input, 2:WorkDesc work ),
	WorkStatus workStatus( 1:string jobID )
}

service RemusWorkServer {
	void emitWork( 1:AppletRef applet, 2:i64 jobID, 3:i64 emitID, 4:string key 5:string data ),
	void errorWork( 1:AppletRef applet, 2:i64 jobID, 3:string message)
}
