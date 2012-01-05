
import os
import json

class WriteTable(object):
    def __init__(self, db, table_ref):
        self.db = db
        self.table_ref = table_ref

    def close(self):
        self.handle = None

    def emit(self, key, value):
        self.db.addData(self.table_ref, key, value)
    
    def copyTo(self, path, key, name):
        self.db.copyTo(path, self.table_ref, key, name)

class ReadTable(object):
    def __init__(self, db, table_ref):
        self.db = db
        self.table_ref = table_ref
    
    def __iter__(self):
        return self.db.listKeyValue(self.table_ref).__iter__()
