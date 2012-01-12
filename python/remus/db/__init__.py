"""

Example of database scanning::
            
    #!/usr/bin/env python

    import sys
    import remus.db

    db = remus.db.connect(sys.argv[1])

    for inst in db.listInstances():
        for table in db.listTables(inst):
            meta = db.getTableInfo(table)
            if 'datatype' in meta and meta['datatype'] == 'cgdata':
                for key in db.listKeys(table):
                    print key




"""


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
    """
    All tables in Remus identified by two values:
    
    1. The instance : A UUID code the identifies a specific pipeline run
    2. The TablePath : A path to a particular table
    
    In string form, there two items are seperated with a colin, ie::
        
        e02d038d-98a3-494a-9a27-c04b4516ced4:/test/myTable
    
    The table path heiarchy represents the chain of targets and their
    children. The top level directory is the original submission name.
    Every time a 'addChildTarget' is called on a target a new directory 
    is created.
    
    The submission code::
        
       instance = manager.submit('submit_20120110', 'mymodule.Submit', {'count' : 5} )
    
    Would create the table directory::
        
        e02d038d-98a3-494a-9a27-c04b4516ced4:/submit_20120110
    
    The target code in mymodule.py ::
        
        class Submit(remus.SubmitTarget):            
            def run(self, params):                
                self.addChildTarget('the_child', MyChildTarget(params['count']))
        
    Would create the table directory::
        
        e02d038d-98a3-494a-9a27-c04b4516ced4:/submit_20120110/the_child
    
    The child target call::
        
        class MyChildTarget(remus.Target):
            
            def __init__(self, count):
                self.count = count
            
            def run(self):
                oTable = self.createTable('outTable')
                
                for i in range(self.count):
                    oTable.emit("test_%d" % (i), i)
                
                oTable.close()
    
    Would create the output table::
        
        e02d038d-98a3-494a-9a27-c04b4516ced4:/submit_20120110/the_child/outTable
    
    And the table would have the key pairs emitted during the for loop
    
    ======  =====
    key     value
    ======  =====
    test_0  0
    test_1  1     
    test_2  2     
    test_3  3     
    test_4  4     
    ======  =====
    
    A :class:`remus.db.TableRef` automatically parses table path strings
    into a common class
    
    
    Example 1::
        
        t = remus.db.TableRef('e02d038d-98a3-494a-9a27-c04b4516ced4', '/submit_20120110/the_child/outTable')
    
    Example 2::
        
        t = remus.db.TableRef('e02d038d-98a3-494a-9a27-c04b4516ced4:/submit_20120110/the_child/outTable')
    
    In both cases, t is the same

    """
    
    
    def __init__(self, instance, table=None):
        if table is None and instance.count(":"):
            tmp = instance.split(":")
            self._instance = tmp[0]
            self._table = tmp[1]
        else:
            self._instance = instance
            self._table = table
    
    def __hash__(self):
        return hash(self.toPath())
    
    def __eq__(self, other):
        return str(self) == str(other)
    
    @property
    def instance(self):
        """
        Instance UUID string
        """
        return self._instance
    
    @property
    def table(self):
        """
        Table path
        """
        return self._table
    
    def __str__(self):
        return "%s:%s" % (self._instance, self._table)
       
    def toPath(self):
        """
        Get the string representation of the table reference
        """
        return "%s:%s" % (self._instance, self._table)


class DBBase:
    """
    Base RemusDB interface. This interface can be implemented via file system
    ie :class:`remus.db.FileDB` or via network database interface
    """
    
    def getPath(self):
        """
        Get the connection URL of the server
        """
        raise NotImplementedException()
        
    
    def createInstance(self, instance):
        """
        Create an instance in the database
        """
        raise NotImplementedException()
    
    def hasTable(self, tableRef):
        """
        Check for existance of table
        """
        raise NotImplementedException()

    def createTable(self, tableRef, tableInfo):
        """
        Create a table in a given instance
        """
        raise NotImplementedException()
    
    def getTableInfo(self, tableRef):
        """
        Get table information
        """
        raise NotImplementedException()        
    
    def addData(self, table, key, value):
        """
        Add data to table
        """
        raise NotImplementedException()

    def getValue(self, table, key):
        """
        Get values from table associated with key
        """
        raise NotImplementedException()
    
    def listKeyValue(self, table):
        """
        List the key value pairs stored in a table
        """
        raise NotImplementedException()

    def listKeys(self, table):
        """
        List all of the keys in a table
        """
        raise NotImplementedException()
    
    def hasKey(self, table, key):
        """
        Check if table has a key
        """
        raise NotImplementedException()
    
    def listInstances(self):
        """
        List instances found in a database
        """
        raise NotImplementedException()
    
    def listTables(self, instance):
        """
        List tables associated with an instance
        """
        raise NotImplementedException()
        
    def hasAttachment(self, table, key, name):
        """
        Check if named attachment for a key exists.
        """
        raise NotImplementedException()

    def listAttachments(self, table, key):
        """
        List attachments for a given key
        """
        raise NotImplementedException()

    def copyTo(self, path, table, key, name):
        """
        Copy file to attachment associated to a key
        """
        raise NotImplementedException()
    
    def copyFrom(self, path, table, key, name):
        """
        Copy file from attachment associated to a key
        """
        raise NotImplementedException()




dbType = {
"file" : "remus.db.FileDB"
}

def connect(path):
    """
    Connect to a Remus Database
    
    :param path: A string describing the address of the database, ie file://data_dir or remus://server01:16020
    
    :returns: :class:`remus.db.DBBase`
    
    """
    tmp = path.split("://")
    if tmp[0] in dbType:
        className = dbType[tmp[0]]
        tmp1 = className.split('.')
        mod = __import__(".".join(tmp1[:-1]))
        cls = mod
        for a in tmp1[1:]:
            cls = getattr(cls, a)
        return cls(tmp[1])        
    return None
    

def path_quote(word):
    return quote(word).replace("/", '%2F')

class FileDB(DBBase):
    """
    Implementation of RemusDB API using a file system
    """
    def __init__(self, basedir):
        self.basedir = os.path.abspath(basedir)
        self.out_handle = {}
    
    def getPath(self):
        return "file://" + self.basedir
    
    def hasTable(self, tableRef):
        fsPath = self._getFSPath(tableRef)
        return os.path.exists(fsPath)            

    def getTableInfo(self, tableRef):
        path = self._getFSPath(tableRef) + "@info"
        if not os.path.exists(path):
            return {}
        handle = open(path)
        data = json.loads(handle.read())
        handle.close()
        return data

    def createTable(self, tableRef, tableInfo):
        fsDir = os.path.dirname(self._getFSPath(tableRef))
        if not os.path.exists(fsDir):
            os.makedirs(fsDir)
        handle = open(self._getFSPath(tableRef) + "@info", "w")
        handle.write(json.dumps(tableInfo))
        handle.close()    
    
    def _getFSPath(self, table):
        return os.path.join(self.basedir, table.instance, re.sub(r'^/', '', table.table))
    
    def addData(self, table, key, value):
        if table not in self.out_handle:
            fspath = self._getFSPath(table)
            self.out_handle[table] = tempfile.NamedTemporaryFile(dir=os.path.dirname(fspath), prefix=os.path.basename(table.table) + "@data.", delete=False)
        self.out_handle[table].write( key )
        self.out_handle[table].write( "\t" )
        self.out_handle[table].write(json.dumps(value))
        self.out_handle[table].write( "\n" )
        self.out_handle[table].flush() 

    def getValue(self, table, key):
        path = self._getFSPath(table)
        out = []
        for path in glob(path + "@data" + "*"):
            handle = open(path)
            for line in handle:
                tmp = line.split("\t")
                if tmp[0] == key:
                    out.append(json.loads(tmp[1]))
            handle.close()
        return out
    
    def listKeyValue(self, table):
        path = self._getFSPath(table)
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
        fspath = self._getFSPath(table)
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
    
    
    def _dirscan(self, dir, inst):
        out = {}
        for path in glob(os.path.join(dir, "*")):
            if path.count("@data"):
                tableName = re.sub(os.path.join(self.basedir, inst), "", re.sub("@data.*", "", path))
                tableRef = TableRef(inst, tableName)
                out[ tableRef ] = True
            if os.path.isdir(path):
                for a in self._dirscan( path, inst ):
                    out[a] = True
        return out.keys()
            
    
    def listTables(self, instance):
        out = self._dirscan( os.path.abspath(os.path.join(self.basedir, instance) ), instance )
        return out
        
    def hasAttachment(self, table, key, name):
        path = self._getFSPath(table)
        attachPath = os.path.join(path + "@attach", key, name)
        return os.path.exists(attachPath)

    def listAttachments(self, table, key):
        path = self._getFSPath(table)
        attachPath = os.path.join( path + "@attach", key)
        for path in glob( os.path.join(attachPath, "*") ):
            yield unquote(os.path.basename(path))

    def copyTo(self, path, table, key, name):
        fspath = self._getFSPath(table)
        attachPath = os.path.join( fspath + "@attach", key, path_quote(name))
        keyDir = os.path.dirname(attachPath)
        if not os.path.exists(keyDir):
            os.makedirs(keyDir)
        shutil.copy( path, attachPath )
    
    def copyFrom(self, path, table, key, name):
        fspath = self._getFSPath(table)
        attachPath = os.path.join( fspath + "@attach", key, path_quote(name))
        shutil.copy( attachPath, path )
    
