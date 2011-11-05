
namespace java org.remus.thrift
namespace py remus.net

const string STATIC_INSTANCE =  "00000000-0000-0000-0000-000000000000";
const string PIPELINE_APPLET = "@pipeline";
const string INSTANCE_APPLET = "@instance";
const string SUBMIT_APPLET = "@submit";
const string WORK_APPLET = "@work";
const string ERROR_APPLET = "@error";
const string DONE_APPLET = "@done";
const string ROOT_PIPELINE = "@root";

struct InstanceRef {
	1: required string pipeline,
	2: required string instance
}

struct AppletRef {
	1: required string pipeline,
	2: required string instance,
	3: required string applet
	4: optional list<string> keys
}

enum WorkMode {
	SPLIT,
	MAP,
	REDUCE,
	PIPE,
	MATCH,
	MERGE,
	REMAP,
	REREDUCE,
	STORE,
	OUTPUT,
	AGENT
}

enum JobState {
	QUEUED,
	WORKING,
	DONE,
	ERROR,
	UNKNOWN
}

enum PeerType {
	MANAGER,
	NAME_SERVER,
	DB_SERVER,
	ATTACH_SERVER,
	WORKER,
	WEB_SERVER,
	DEAD
}

struct PeerAddress {
	1:required string host;
	2:required i32 port;
}

struct PeerInfoThrift {
	1:required PeerType peerType;
	2:required string name;
	3:optional string peerID;
	4:optional string groupName;
	5:optional list<string> workTypes;
	6:optional string configJSON;
	7:optional PeerAddress addr;
	8:optional i32 timeDelta;
}

struct WorkDesc {
	1: required string lang;
	2: required WorkMode mode;
	3: required string infoJSON;
	4: required AppletRef workStack;
	5: required i64 workStart;
	6: required i64 workEnd;
}

struct KeyValJSONPair {
	1:required string key,
	2:required string valueJson,
	3:required i64 jobID,
	4:required i64 emitID
}

struct JobStatus {
	1:required JobState status;
	2:optional i64 emitCount;
	3:optional string errorMsg;
}

exception NotImplemented {

}

exception BadPeerName {

}


service RemusNet {
	
	string status();
	
	/**
	 * Data access related methods
	 *
	 */
	
	bool containsKey( 1:AppletRef stack, 2:string key ) throws (1:NotImplemented e);
	
	list<string> keySlice( 1:AppletRef stack, 2:string keyStart, 3:i32 count ) throws (1:NotImplemented e);
	
	list<string> getValueJSON( 1:AppletRef stack, 2:string key ) throws (1:NotImplemented e);

	i64 keyCount( 1:AppletRef stack, 2:i32 maxCount ) throws (1:NotImplemented e);

	void addDataJSON( 1:AppletRef stack, 2:i64 jobID, 3:i64 emitID, 4:string key, 5:string data) throws (1:NotImplemented e);

	list<KeyValJSONPair> keyValJSONSlice( 1:AppletRef stack, 2:string startKey, 3:i32 count) throws (1:NotImplemented e);

	void deleteStack( 1:AppletRef stack ) throws (1:NotImplemented e);
	
	void deleteValue( 1:AppletRef stack, 2:string key ) throws (1:NotImplemented e);
	
	list<AppletRef> stackSlice(1:string startKey, 2:i32 count) throws (1:NotImplemented e);
	
	i64 getTimeStamp( 1:AppletRef stack ) throws (1:NotImplemented e);

	/**
	 * Work results methods
	 *
	 */

	//void emitWork( 1:string workerID, 2:AppletRef applet, 3:i64 jobID, 4:i64 emitID, 5:string key 6:string data ) throws (1:NotImplemented e);
	//void errorWork( 1:string workerID, 2:AppletRef applet, 3:i64 jobID, 4:string message) throws (1:NotImplemented e);

	/**
	 * Attachment methods
	 *
	 */

	void initAttachment(1:AppletRef stack, 2:string key, 3:string name) throws (1:NotImplemented e);
	
	i64 getAttachmentSize(1:AppletRef stack, 2:string key, 3:string name)  throws (1:NotImplemented e);
	
	binary readBlock( 1:AppletRef stack, 2:string key, 3:string name, 4:i64 offset, 5:i32 length ) throws (1:NotImplemented e);
	
	void appendBlock( 1:AppletRef stack, 2:string key, 3:string name, 4:binary data )  throws (1:NotImplemented e);
	
	list<string> listAttachments( 1:AppletRef stack, 2:string key ) throws (1:NotImplemented e);

	bool hasAttachment( 1:AppletRef stack, 2:string key, 3:string name) throws (1:NotImplemented e);

	void deleteAttachment( 1:AppletRef stack, 2:string key, 3:string name ) throws (1:NotImplemented e);
	
	/**
	 * Worker methods
	 *
	 */
	
	string jobRequest( 1:string dataServer, 2:string attachServer, 3:WorkDesc work ) throws (1:NotImplemented e);
	JobStatus jobStatus( 1:string jobID ) throws (1:NotImplemented e);
	i32 jobCancel( 1:string jobID ) throws (1:NotImplemented e);

	/**
	 * Manager methods
	 */
	void scheduleRequest() throws (1:NotImplemented e); 
	string scheduleInfoJSON() throws (1:NotImplemented e);

	/**
	 * Name service methods
	 *
	 */
	list<PeerInfoThrift> peerInfo( 1:list<PeerInfoThrift> info ) throws (1:NotImplemented notImp, 2:BadPeerName badName);

}

