
namespace cpp remusNet
namespace java org.remusNet.thrift
namespace php remusNet
namespace perl remusNet
namespace py remusNet

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
	MERGE
}

enum WorkStatus {
	QUEUED,
	WORKING,
	DONE,
	ERROR
}

struct PeerInfo {
	1:optional bool master;
	2:optional bool worker;
	3:optional bool database;
	4:optional bool attachment;
	5:required string name;
	6:required string address;
	7:required i32    port;
}

struct WorkDesc {
	1: required string lang,
	2: required WorkMode mode,
	3: required AppletRef input,
	4: required AppletRef output
}

struct KeyValJSONPair {
	1:required string key,
	2:required string valueJson,
	3:required i64 jobID,
	4:required i64 emitID
}

service RemusDBThrift {
	
	/**
	 * Data access related methods
	 *
	 */
	
	bool containsKey( 1:AppletRef stack, 2:string key );
	
	list<string> keySlice( 1:AppletRef stack, 2:string keyStart, 3:i32 count );
	
	list<string> getValueJSON( 1:AppletRef stack, 2:string key );

	i64 keyCount( 1:AppletRef stack, 2:i32 maxCount );

	void addData( 1:AppletRef stack, 2:i64 jobID, 3:i64 emitID, 4:string key, 5:string data);

	list<KeyValJSONPair> keyValJSONSlice( 1:AppletRef stack, 2:string startKey, 3:i32 count);

	void deleteStack( 1:AppletRef stack );
	
	void deleteValue( 1:AppletRef stack, 2:string key );
	
	i64 getTimeStamp( 1:AppletRef stack );

	/**
	 * Work results methods
	 *
	 */

	//void emitWork( 1:string workerID, 2:AppletRef applet, 3:i64 jobID, 4:i64 emitID, 5:string key 6:string data );
	//void errorWork( 1:string workerID, 2:AppletRef applet, 3:i64 jobID, 4:string message);


}

service RemusAttachThrift {

	void initAttachment( 1:AppletRef stack, 2:string key, 3:string name, 4:i64 length),
	
	i64 getAttachmentSize( 1:AppletRef stack, 2:string key, 3:string name),
	
	binary readBlock( 1:AppletRef stack, 2:string key, 3:string name, 4:i64 offset, 5:i32 length ),
	
	void writeBlock( 1:AppletRef stack, 2:string key, 3:string name, 4:i64 offset, 5:binary data ),
	
	list<string> listAttachments( 1:AppletRef stack, 2:string key ),

	bool hasAttachment( 1:AppletRef stack, 2:string key, 3:string name),

	void deleteAttachment( 1:AppletRef stack, 2:string key, 3:string name ),	

	void deleteStack( 1:AppletRef stack )
	
}


service RemusWorker {
	string startJob( 1:string dataServer, 2:AppletRef input, 3:WorkDesc work ),
	WorkStatus workStatus( 1:string jobID )
}

exception BadPeerName {

}


service RemusGossip {
	void addPeer( 1:PeerInfo info ) throws (1:BadPeerName e);
	void delPeer( 1:string peerName );
	list<PeerInfo> getPeers();
	void ping( 1:list<PeerInfo> workers );

}
