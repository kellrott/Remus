
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
const string WORKSTAT_APPLET = "@workstat";

struct TableRef {
	1: required string instance,
	2: required string table
}


struct KeyValJSONPair {
	1:required string key,
	2:required string valueJson,
	3:required i64 jobID,
	4:required i64 emitID
}


struct AttachmentInfo {
	1:required string name;
	2:optional i64 size;
	3:optional bool exists;
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
	
	bool containsKey( 1:TableRef table, 2:string key ) throws (1:NotImplemented e);
	
	list<string> keySlice( 1:TableRef table, 2:string keyStart, 3:i32 count ) throws (1:NotImplemented e);
	
	list<string> getValueJSON( 1:TableRef table, 2:string key ) throws (1:NotImplemented e);

	i64 keyCount( 1:TableRef table, 2:i32 maxCount ) throws (1:NotImplemented e);

	void addDataJSON( 1:TableRef table, 2:string key, 3:string data) throws (1:NotImplemented e);

	list<KeyValJSONPair> keyValJSONSlice( 1:TableRef table, 2:string startKey, 3:i32 count) throws (1:NotImplemented e);

	void createTable( 1:TableRef table ) throws (1:NotImplemented e);

	void deleteTable( 1:TableRef table ) throws (1:NotImplemented e);
		
	list<string> stackSlice(1:string startKey, 2:i32 count) throws (1:NotImplemented e);
		
	/**
	 * Attachment methods
	 *
	 */

	void initAttachment(1:TableRef table, 2:string key, 3:string name) throws (1:NotImplemented e);
	
	AttachmentInfo getAttachmentInfo(1:TableRef table, 2:string key, 3:string name) throws (1:NotImplemented e);
	
	binary readBlock( 1:TableRef table, 2:string key, 3:string name, 4:i64 offset, 5:i32 length ) throws (1:NotImplemented e);
	
	void appendBlock( 1:TableRef table, 2:string key, 3:string name, 4:binary data )  throws (1:NotImplemented e);
	
	list<string> listAttachments( 1:TableRef table, 2:string key ) throws (1:NotImplemented e);

	bool hasAttachment( 1:TableRef table, 2:string key, 3:string name) throws (1:NotImplemented e);

	void deleteAttachment( 1:TableRef table, 2:string key, 3:string name ) throws (1:NotImplemented e);
	
}

