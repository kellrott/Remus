
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
    def __init__(self, instance, table=None):
        if table is None and instance.count(":"):
            tmp = instance.split(":")
            self.instance = tmp[0]
            self.table = tmp[1]
        else:
            self.instance = instance
            self.table = table
    
    def __str__(self):
        return "%s:%s" % (self.instance, self.table)
       
    def toPath(self):
        return "%s:%s" % (self.instance, self.table)


class DBBase:
        
    def createInstance(self, instance):
        raise NotImplementedException()

def path_quote(word):
    return quote(word).replace("/", '%2F')

class FileDB(DBBase):
    def __init__(self, basedir):
        self.basedir = os.path.abspath(basedir)
        self.out_handle = {}
    
    def hasTable(self, tableRef):
        fsPath = self.getFSPath(tableRef)
        print "checking", fsPath
        return os.path.exists(fsPath)            

    def createTable(self, tableRef):
        fsDir = os.path.dirname(self.getFSPath(tableRef))
        if not os.path.exists(fsDir):
            os.makedirs(fsDir)
    
    
    def getFSPath(self, table):
        return os.path.join(self.basedir, table.instance, re.sub(r'^/', '', table.table))
    
    def addData(self, table, key, value):
        if table not in self.out_handle:
            fspath = self.getFSPath(table)
            print "open_tmp", fspath
            self.out_handle[table] = tempfile.NamedTemporaryFile(dir=os.path.dirname(fspath), prefix=os.path.basename(table.table) + "@data.", delete=False)
        self.out_handle[table].write( key )
        self.out_handle[table].write( "\t" )
        self.out_handle[table].write(json.dumps(value))
        self.out_handle[table].write( "\n" )
        self.out_handle[table].flush() 

    def getValue(self, table, key):
        path = self.getFSPath(table)
        out = []
        for path in glob(path + "@data" + "*"):
            print "scanning:", path, key
            handle = open(path)
            for line in handle:
                tmp = line.split("\t")
                if tmp[0] == key:
                    out.append(json.loads(tmp[1]))
            handle.close()
        return out
    
    def listKeyValue(self, table):
        path = self.getFSPath(table)
        out = []
        for path in glob(path + "@data" + "*"):
            handle = open(path)
            for line in handle:
                tmp = line.split("\t")
                out.append( (tmp[0], json.loads(tmp[1]) ) )
            handle.close()
        return out

    def listKeys(self, table):
        out = []
        fspath = self.getFSPath(table)
        for path in glob(fspath + "@data" + "*"):
            handle = open(path)
            for line in handle:
                tmp = line.split("\t")
                out.append(tmp[0])
            handle.close()
        return out
    
    def hasKey(self, table, key):
        o = self.listKeys(table)
        return key in o
    
    def listInstances(self):
        out = []
        for path in glob(os.path.join(self.basedir, "*")):
            out.append(os.path.basename(path))
        return out
    
    
    def __dirscan__(self, dir, inst):
        out = {}
        for path in glob(os.path.join(dir, "*")):
            if path.count("@data"):
                out[ re.sub( os.path.join(self.basedir, inst), "", re.sub("@data.*", "", path) ) ] = True
            if os.path.isdir(path):
                for a in self.__dirscan__( path, inst ):
                    out[a] = True
        return out.keys()
            
    
    def listTables(self, instance):
        out = self.__dirscan__( os.path.abspath(os.path.join(self.basedir, instance) ), instance )
        print "dirscan", out
        return out
        
    def hasAttachment(self, table, key, name):
        path = self.getFSPath(table)
        attachPath = os.path.join(path + "@attach", key, name)
        return os.path.exists(attachPath)

    def listAttachments(self, table, key):
        path = self.getFSPath(table)
        attachPath = os.path.join( path + "@attach", key)
        for path in glob( os.path.join(attachPath, "*") ):
            yield unquote(os.path.basename(path))

    def copyTo(self, path, table, key, name):
        fspath = self.getFSPath(table)
        attachPath = os.path.join( fspath + "@attach", key, path_quote(name))
        keyDir = os.path.dirname(attachPath)
        if not os.path.exists(keyDir):
            os.makedirs(keyDir)
        shutil.copy( path, attachPath )
    
    def copyFrom(self, path, table, key, name):
        fspath = self.getFSPath(table)
        attachPath = os.path.join( fspath + "@attach", key, path_quote(name))
        shutil.copy( attachPath, path )
    
