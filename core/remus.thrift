
namespace java org.remus.thrift
namespace py remus.net

const string STATIC_INSTANCE =  "00000000-0000-0000-0000-000000000000";
const string ERROR_APPLET = "@error";
const string DONE_APPLET = "@done";

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

enum TableStatus
{
  READY = 1,
  NOT_AVAILABLE,
  NOT_SYNCED,
}

exception TableError {
1: string message;
}

service RemusNet {
	
	/**
	 * Data access related methods
	 *
	 */
	
	bool containsKey( 1:TableRef table, 2:string key );
	
	list<string> keySlice( 1:TableRef table, 2:string keyStart, 3:i32 count );
	
	list<string> getValueJSON( 1:TableRef table, 2:string key );

	bool hasKey( 1:TableRef table, 2:string key );

	void addDataJSON( 1:TableRef table, 2:string key, 3:string data) throws(1:TableError err);

	list<KeyValJSONPair> keyValJSONSlice( 1:TableRef table, 2:string startKey, 3:i32 count);
	
	void createInstanceJSON( 1:string instance, 2:string instanceJSON );

	void createTableJSON( 1:TableRef table, 2:string tableJSON );

    bool hasTable(1:TableRef table);
    
    list<TableRef> listTables(1:string instance);
    
    list<string> listInstances();
    
    void deleteInstance( 1:string instance);
	void deleteTable( 1:TableRef table );

	bool syncTable( 1:TableRef table );
	
	TableStatus tableStatus( 1:TableRef table );
		
	list<string> tableSlice(1:string startKey, 2:i32 count);
		
	/**
	 * Attachment methods
	 *
	 */

	void initAttachment(1:TableRef table, 2:string key, 3:string name);
	
	AttachmentInfo getAttachmentInfo(1:TableRef table, 2:string key, 3:string name);
	
	binary readBlock( 1:TableRef table, 2:string key, 3:string name, 4:i64 offset, 5:i32 length );
	
	void appendBlock( 1:TableRef table, 2:string key, 3:string name, 4:binary data );
	
	list<string> listAttachments( 1:TableRef table, 2:string key );

	bool hasAttachment( 1:TableRef table, 2:string key, 3:string name);

	void deleteAttachment( 1:TableRef table, 2:string key, 3:string name );
	
}

