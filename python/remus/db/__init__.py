
import os
import json
from glob import glob
import shutil
from urllib import quote, unquote

class NotImplementedException(Exception):
    def __init__(self):
        Exception.__init__(self)


class TableRef(object):
    def __init__(self, instance, table):
        self.instance = instance
        self.table = table


class DBBase:
        
    def createInstance(self, instance):
        raise NotImplementedException()

def path_quote(word):
    return quote(word).replace("/", '%2F')

class FileDB(DBBase):
    def __init__(self, basedir):
        self.basedir = os.path.abspath(basedir)

    def createTable(self, tableRef):
        instDir = os.path.join( self.basedir, str(tableRef.instance))
        if not os.path.exists(instDir):
            os.makedirs(instDir)
    
    
    def addData(self, table, key, value):
        handle = open(os.path.join(self.basedir, table.instance, table.table + "@data"), "w")
        handle.write( key )
        handle.write( "\t" )
        handle.write(json.dumps(value))
        handle.close()
    
    def getValue(self, table, key):
        handle = open(os.path.join(self.basedir, table.instance, table.table + "@data"))
        out = []
        for line in handle:
            tmp = line.split("\t")
            if tmp[0] == key:
                out.append(json.loads(tmp[1]))
        handle.close()
        return out

    def listAttachments(self, table, key):
        attachPath = os.path.join( self.basedir, table.instance, table.table, key)
        for path in glob( os.path.join(attachPath, "*") ):
            yield unquote(os.path.basename(path))

    def copyTo(self, path, table, key, name):
        attachPath = os.path.join( self.basedir, table.instance, table.table, key, path_quote(name))
        keyDir = os.path.dirname(attachPath)
        if not os.path.exists(keyDir):
            os.makedirs(keyDir)
        shutil.copy( path, attachPath )
    
    def copyFrom(self, path, table, key, name):
        attachPath = os.path.join( self.basedir, table.instance, table.table, key, path_quote(name))
        shutil.copy( attachPath, path )
    
