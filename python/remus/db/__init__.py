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
import tempfile
import socket
import threading
import time
import sys
import errno

try:
    from remus.net.ttypes import TableRef as TableRefBase
except ImportError:
    TableRefBase = object
    
    class NotImplementedException(Exception):
        def __init__(self):
            Exception.__init__(self)
    
    class TableError(Exception):
        def __init__(self, message):
            Exception.__init__(self, message)

class TableRef(TableRefBase):
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
        if not self._table.startswith("/"):
            self._table = "/" + self._table
    
    def __repr__(self):
        return repr( (self._instance, self._table) )
    
    def __cmp__(self, other):
        return cmp(str(self), str(other))
    
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

def join(*args):
    """
    Join togeather series of strings and TableRefs to build absolute TableRef
    
    :params args: :class:`remus.db.TableRef` and strings
    
    If the first argument is a string, it is assumed to be the instance reference
    
    Valid examples
    
    Example 1::
        
        > inst_a = "f227a0af-826a-4617-90ee-136acbd42715"
        > ref_a = remus.db.TableRef(inst_a, "tableTest")
        > str(remus.db.join(ref_a, "child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/tableTest/child_a"
        True
    
    Example 2::
        
        > inst_a = "f227a0af-826a-4617-90ee-136acbd42715"
        > ref_a = remus.db.TableRef(inst_a, "tableTest")
        str(remus.db.join(ref_a, "..", "child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/child_a"
        
    Example 3::
        
        > inst_a = "f227a0af-826a-4617-90ee-136acbd42715"
        > ref_a = remus.db.TableRef(inst_a, "tableTest")        
        > str(remus.db.join(ref_a, "/child_a")) == "f227a0af-826a-4617-90ee-136acbd42715:/child_a"
        
    Example 4::
        
        > inst_a = "f227a0af-826a-4617-90ee-136acbd42715"
        > inst_b = "a061bfac-987e-4aa8-a3d5-567352b09ed3"
        > ref_a = remus.db.TableRef(inst_a, "tableTest")        
        > ref_b = remus.db.TableRef(inst_b, "child_1")
        > remus.db.join(ref_a, ref_b) == "a061bfac-987e-4aa8-a3d5-567352b09ed3:/child_1"     
        True
        > remus.db.join(ref_a, "a061bfac-987e-4aa8-a3d5-567352b09ed3:/child_1") == "a061bfac-987e-4aa8-a3d5-567352b09ed3:/child_1"
        True
        > remus.db.join("f227a0af-826a-4617-90ee-136acbd42715:/tableTest/child_a", "a061bfac-987e-4aa8-a3d5-567352b09ed3:/child_1") == "a061bfac-987e-4aa8-a3d5-567352b09ed3:/child_1"
        True
        
    """
    inst = None
    path = []
    for a in args:
        if isinstance(a, TableRef):
            inst = a.instance
            path = a.table.split("/")
        elif a.count(":"):
            tmp = a.split(":")
            inst = tmp[0]
            path = tmp[1].split("/")
        else:
            if inst is None:
                inst = a
            else:
                path.append(a)
    return TableRef(inst, os.path.abspath(os.path.join("/", *path)))

def basename(path):
    return os.path.basename(str(path))

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
        
    
    def createInstance(self, instance, instanceInfo):
        """
        Create an instance in the database
        """
        raise NotImplementedException()

    def getInstanceInfo(self, instance):
        """
        Get info about an instance
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
    
    def createFile(self, path, fileInfo):
        """
        Create a file in a given instance
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

    def copyTo(self, path, table, key=None, name=None):
        """
        Copy file to attachment associated to a key
        """
        raise NotImplementedException()
    
    def copyFrom(self, path, table, key=None, name=None):
        """
        Copy file from attachment associated to a key
        """
        raise NotImplementedException()
    
    def readAttachment(self, table, key, name):
        """
        Get file like handle to read attachment
        """
        raise NotImplementedException()
    




dbType = {
"file" : "remus.db.filedb.FileDB",
"remus" : "remus.db.thrift_net.Client"
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
    
