
import os
import json

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

class ReadTable(object):
    def __init__(self, db, table_ref):
        self.db = db
        self.table_ref = table_ref
    
    def __iter__(self):
        """
        Iterator through stack and return sets of key, value pairs
        """
        return self.db.listKeyValue(self.table_ref).__iter__()
    
    def get(self, key):
        return self.db.getValue(self.table_ref, key)
    
    def copyFrom(self, path, key, name):
        return self.db.copyFrom(path, self.table_ref, key, name)
