
import os
import json
import remus.db

class WriteTable(object):
    """
    A WriteTable is used for output, and can only be written to.
    """
    def __init__(self, db, table_ref):
        self.db = db
        self.table_ref = table_ref

    def close(self):
        """
        Properly flush and close table
        """
        self.handle = None

    def emit(self, key, value):
        """
        Emit a key value pair
        
        :param key:
            Key to be emited
            
        :param value:
            Value to be emitted. Note, this value must be :func:`json.dumps` serialisable
        """
        self.db.addData(self.table_ref, key, value)
    
    def copyTo(self, path, key, name):
        """
        Copy a file to the output table
        
        :param path:
            Path of the file to be copied
            
        :param key:
            Key the file will be associated with
            
        :param name:
            Name the attachment will be stored as
        """
        self.db.copyTo(path, self.table_ref, key, name)


    def getPath(self):
        """
        Get absolute path of table
        
        :returns: Sting of table's absolute path
        """

        return self.table_ref.toPath()
        
class ReadTable(object):
    def __init__(self, db, table_ref):
        self.db = db
        self.table_ref = table_ref
        if not self.db.hasTable(table_ref):
            raise remus.db.TableError("Table not found:" + str(table_ref))    
    
    def __iter__(self):
        """
        Iterator through stack and return sets of key, value pairs
        """
        return self.db.listKeyValue(self.table_ref).__iter__()
    
    def get(self, key):
        """
        Get values associated with key
        
        :param key: Key to get
        
        :returns: List of values associated with key
        """
        return self.db.getValue(self.table_ref, key)
    
    def hasKey(self, key):
        """
        Does table have the key?
        
        :param key: the key
        
        :returns: Boolean
        """
        return self.db.hasKey(self.table_ref, key)
    
    def copyFrom(self, path, key, name):
        """
        Copy file from table
        
        :param path: Path to store file at:
        
        :param key: Key to copy attachment from
        
        :param name: Name of the attachment
        
        """
        return self.db.copyFrom(path, self.table_ref, key, name)

    def readAttachment(self, key, name):
        """
        Get file like handle to read from attachment
        
        :param path: Path to store file at:
        
        :param key: Key to copy attachment from
        
        :param name: Name of the attachment
        
        """
        return self.db.readAttachment(self.table_ref, key, name)
    
    def listKeys(self):
        """
        List keys in table
        """
        return self.db.listKeys(self.table_ref)

    def getPath(self):
        """
        Get absolute path of table
        
        :returns: Sting of table's absolute path
        """
        return self.table_ref.toPath()
