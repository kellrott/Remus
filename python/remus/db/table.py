
import os
import json

class KeyTable(object):
    def __init__(self, db, table_ref):
        self.db = db
        self.table_ref = table_ref

    def close(self):
        self.handle = None

    def emit(self, key, value):
        self.db.addData(self.table_ref, key, value)
