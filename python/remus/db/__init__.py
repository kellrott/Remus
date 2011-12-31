
import os
import json
import re
from glob import glob
import shutil
from urllib import quote, unquote
import tempfile

class NotImplementedException(Exception):
    def __init__(self):
        Exception.__init__(self)


class TableRef(object):
    def __init__(self, instance, table):
        self.instance = instance
        self.table = table
    
    def __str__(self):
        return "%s:%s" % (self.instance, self.table)


class DBBase:
        
    def createInstance(self, instance):
        raise NotImplementedException()

def path_quote(word):
    return quote(word).replace("/", '%2F')

class FileDB(DBBase):
    def __init__(self, basedir):
        self.basedir = os.path.abspath(basedir)
        self.out_handle = None

    def createTable(self, tableRef):
        instDir = os.path.join( self.basedir, str(tableRef.instance))
        if not os.path.exists(instDir):
            os.makedirs(instDir)
    
    
    def addData(self, table, key, value):
        if self.out_handle is None:
            self.out_handle = tempfile.NamedTemporaryFile(dir=os.path.join(self.basedir, table.instance), prefix=table.table + "@data.", delete=False)
        self.out_handle.write( key )
        self.out_handle.write( "\t" )
        self.out_handle.write(json.dumps(value))
        self.out_handle.write( "\n" )
        self.out_handle.flush() 

    def getValue(self, table, key):
        out = []
        for path in glob(os.path.join(self.basedir, table.instance, table.table + "@data") + "*"):
            handle = open(path)
            for line in handle:
                tmp = line.split("\t")
                if tmp[0] == key:
                    out.append(json.loads(tmp[1]))
            handle.close()
        return out

    def listKeys(self, table):
        out = []
        for path in glob(os.path.join(self.basedir, table.instance, table.table + "@data") + "*"):
            handle = open(path)
            for line in handle:
                tmp = line.split("\t")
                out.append(tmp[0])
            handle.close()
        return out
    
    def listInstances(self):
        out = []
        for path in glob(os.path.join(self.basedir, "*")):
            out.append(os.path.basename(path))
        return out
    
    def listTables(self, instance):
        out = {}
        for path in glob(os.path.join(self.basedir, instance, "*@data*")):
            out[ ( re.sub("@data.*", "", os.path.basename(path)) ) ] = True
        return out.keys()
    
        
    def hasAttachment(self, table, key, name):
        attachPath = os.path.join( self.basedir, table.instance, table.table + "@attach", key, name)
        return os.path.exists(attachPath)

    def listAttachments(self, table, key):
        attachPath = os.path.join( self.basedir, table.instance, table.table + "@attach", key)
        for path in glob( os.path.join(attachPath, "*") ):
            yield unquote(os.path.basename(path))

    def copyTo(self, path, table, key, name):
        attachPath = os.path.join( self.basedir, table.instance, table.table + "@attach", key, path_quote(name))
        keyDir = os.path.dirname(attachPath)
        if not os.path.exists(keyDir):
            os.makedirs(keyDir)
        shutil.copy( path, attachPath )
    
    def copyFrom(self, path, table, key, name):
        attachPath = os.path.join( self.basedir, table.instance, table.table + "@attach", key, path_quote(name))
        shutil.copy( attachPath, path )
    
