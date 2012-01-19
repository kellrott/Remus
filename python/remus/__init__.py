import os
import json
from copy import copy

try:
    from remus.net import RemusNet
    from thrift import Thrift
    from thrift.transport import TSocket
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol
    from remus.net import constants
except ImportError:
    pass

from remus.db import FileDB, TableRef
from remus.db import join as db_join


class RemusApplet(object):
    """
    Base Remus Class
    """
    def __init__(self):
        self.__manager__ = None

    def __setmanager__(self, manager):
        self.__manager__ = manager
    
    def __setpath__(self, instance, tablePath):
        self.__instance__ = instance
        self.__tablepath__ = tablePath




class Target(RemusApplet):
    """
    The target class describes a python class to be pickel'd and 
    run as a remote process at a later time.
    """

    def run(self):
        """
        The run method is user provided and run in a seperate process,
        possible on a remote node.
        """
        raise Exception()

    def addChildTarget(self, child_name, child):
        """
        Add child target to be executed
        
        :param child_name:
            Unique name of child to be executed
        
        :param child:
            Target object to be pickled and run as a remote 
        
        
        Example::
            
            class MyWorker(remus.Target):
                
                def __init__(self, dataBlock):
                    self.dataBlock = dataBlock
                
                def run(self):
                    c = MyOtherTarget(dataBlock.pass_to_child)
                    self.addChildTarget('child', c )
        
        
        """
        self.__manager__._addChild(self, child_name, child)
    
    def addFollowTarget(self, child_name, child, depends=None):
        """
        A follow target is a delayed callback, that isn't run until all 
        of a targets children have complete
        """
        if depends is None:
            self.__manager__._addChild(self, child_name, child, depends=self.__tablepath__)
        else:
            ref = db_join(self.__instance__, self.__tablepath__, depends)
            self.__manager__._addChild(self, child_name, child, depends=ref.table)
            

    def createTable(self, tableName, tableInfo={}):
        """
        Create a table to output to

        :param tableName: Name of the table to be opened
        
        :param tableInfo: An object to descibe the characteristics of the table
        
        :return: :class:`remus.db.table.WriteTable`
        
        In the case of a target::
            
            e02d038d-98a3-494a-9a27-c04b4516ced4:/submit_20120110/the_child
        
        The call::
            
            self.openTable('output')
        
        Would create the table::
            
            e02d038d-98a3-494a-9a27-c04b4516ced4:/submit_20120110/the_child/output
       

        """
        return self.__manager__._createTable(self.__instance__, self.__tablepath__ + "/" + tableName, tableInfo)

    def openTable(self, tableName):
        """
        Open a table input from. By default, tables belong to the parents,
        because they have already finished executing
        
        :param tableName: Name of the table to be opened
        
        :return: :class:`remus.db.table.ReadTable`
        
        In the case of a target::
            
            e02d038d-98a3-494a-9a27-c04b4516ced4:/submit_20120110/the_child
        
        The call::
            
            self.openTable('input')
        
        Would open the table::
            
            e02d038d-98a3-494a-9a27-c04b4516ced4:/submit_20120110/input
        
        Because openTable opens in the parent directory (because the parent has already completed)
        
        
        """
        tablePath = db_join(self.__instance__, self.__tablepath__, "..", tableName)
        return self.__manager__._openTable(tablePath.instance, tablePath.table)


class SubmitTarget(Target):
    """
    Submission target
    
    This is a target class, but unlike the original target class, which 
    is generated from a stored pickle, the SubmitTarget receives information from
    a JSON style'd data structure from a submission table.
    This allows for it to be instantiated outside of a python environment, ie from the command 
    line or via web request.
    
    Example pipeline.py::
        
        class PipelineRoot(remus.SubmitTarget):
    
            def run(self, params):
                self.addChildTarget( 'tableScan', GenerateTable(params['tableBase']) )
        
        if __name__ == '__main__':
            config = remus.manage.Config('tmpdir', 'datadir', 'processExecutor')
            manager = remus.manage.Manager(config)    
            instance = manager.submit('test', 'pipeline.PipelineRoot', {'tableBase' : sys.argv[1]} )
            manager.wait()

    
    """
    def __init__(self):
        RemusApplet.__init__(self)
    
    def run(self, params):
        """
        The run method is user provided and run in a seperate process,
        possible on a remote node.
        
        :param params:
            Data structure containing submission data
        """
        raise Exception()

    def openTable(self, tableName):
        i = db_join(self.__instance__, tableName)
        print "open table:", i
        return self.__manager__._openTable(i.instance, i.table)
        
        

class LocalSubmitTarget(Target):
    """
    Local Submit target.
    
    This target class is used to help setup a pipeline run during initialization.
    The primary difference between this class and other, is that it is run 
    on the submission node (as opposed to a remote node), so it has access to the local 
    file system and can be used to setup tables and input data based on the local files,
    before calculation start up on remote nodes. 
    
    Example localSubmit.py::
        
        #this class will be run remotely
        class RemoteChild(remus.Target):
            def run(self):
                o = self.createTable("output")
                o.emit("test", 2)
                o.close()


        #this class will be run locally, during the submission process
        class LocalSubmission(remus.LocalSubmitTarget):    
            def run(self):
                o = self.createTable("output")
                o.emit("test", 1)
                o.close()        
                r = RemoteChild()
                self.addChildTarget('child', r)

        
        if __name__ == '__main__':
            import localSubmit
            config = remus.manage.Config("tmp_dir", 'data_dir', 'processExecutor')
            manager = remus.manage.Manager(config)
            l = localSubmit.LocalSubmission()
            instance = manager.submit('test', l)
    
    Note how in the __main__ block, the LocalSubmission class is referenced 
    by importing localSubmit (the file we're already looking at) and refering to 
    localSubmit.LocalSubmission. This is done to deal with issues in pickle.
    If we refer to LocalSubmission directly, it's class module info is '__main__',
    which will make no sense determining the module that provides the source for
    RemoteChild.
       
    """
    
    def run(self):
        """
        The run method is user provided and run on the local node during the 
        pipeline initialization.
        """
        raise Exception()

    def openTable(self, tableName):
        """
        LocalSubmitTarget inherits openTable from :class:`remus.Target`
        but it doesn't make sence in this context because the target has 
        no parents. It only raises an exception
        
        :raises: Exception
        """
        raise Exception()
        


class TableTarget(Target):
    """
    A target class with an emit method. During initilization, the TableTarget
    expects to get that name of a table to output to. Multiple TableTargets can point 
    to the same output table
    """
    
    def __init__(self, outTable, outTableInfo={}):
        """
        
        :param outTable: Name of the table to output to
        """
        self.__outTableRef__ = outTable
        self.__outTableInfo__ = outTableInfo
        self.__outTable__ = None
    
    def run(self):
        """
        The run method is user provided and run on the local node during the 
        pipeline initialization.
        """
        raise Exception()

    
    def emit(self, key, value):
        """
        Emit a value to be stored in the output table
        """
        if self.__outTable__ is None:
            self.__outTable__ = self.__manager__._createTable(self.__instance__, os.path.abspath( os.path.join(self.__tablepath__, "..", self.__outTableRef__)), self.__outTableInfo__ )
        
        self.__outTable__.emit(key, value)

    def copyTo(self, path, key, name):
        """
        Copy out file
        
        :param path: Path of the file
        
        :param key: Key the file is associated with
        
        :param name: Name of the attachment  
        """
        if self.__outTable__ is None:
            self.__outTable__ = self.__manager__._createTable(self.__instance__, os.path.abspath( os.path.join(self.__tablepath__, "..", self.__outTableRef__)), self.__outTableInfo__ )
        self.__outTable__.copyTo(path, key, name)
    


class MultiApplet(RemusApplet):
    
    def __run__(self):
        raise Exception("Map method not implemented")


class MapTarget(MultiApplet):
    """
    A Target Map operation, which will run on one table.

    Example::
        
        
        class TableMap(remus.MapTarget):            
            def map(self, key, val):
                total = sum(val.values())
                self.emit( key, total / len(val.values()) )
        
        class SubmitRoot(remus.SubmitTarget):
            ...
            Create a table named inputTable
            ...
            self.addChildTarget( 'tableMap', TableMap( 'inputTable') )

    The TableMap child of SubmitRoot will run on the table 'inputTable' and output to 
    the 'tableMap' table
    
    """
    def __init__(self, inputTable, tableInfo={}):
        self.__keyTable__ = inputTable
        self.__outTable__ = None
        self.__tableInfo__ = tableInfo
        
    def __run__(self):
        tpath = os.path.abspath(os.path.join( self.__tablepath__, "..", self.__keyTable__))

        src = self.__manager__._openTable(self.__instance__, tpath)
        for key, val in src:
            self.map(key, val)
            
    def map(self, key, value):
        """
        The user provided method that will be called on each key-value pair in a table
        
        :param key: The key to be mapped
        
        :param value: The value to be mapped
        """
        raise Exception("Map method not implemented")
    
    def emit(self, key, value):
        """
        Emit a value to be stored in the output table
        """
        if self.__outTable__ is None:
            self.__outTable__ = self.__manager__._createTable(self.__instance__, self.__tablepath__, self.__tableInfo__)
        
        self.__outTable__.emit(key, value)

class RemapTarget(MultiApplet):
    
    """
    A RemapTarget operates is a similar fashion to the MapTarget, except it is designed to 
    merge multiple table togeather using a set of mapped permutations.
    
    For example, in the case of three tables that need to be merge via some set of 
    determined permutations, named table_a, table_b and table_c. We also need a table 
    of the valid permutations, table_perm
    
    table_perm takes that form:    

    ================= =============================================
    key               value
    ================= =============================================
    permkey_1         [table_a_key_1, table_b_key_1, table_c_key_1]
    permkey_2         [table_a_key_2, table_b_key_2, table_c_key_2]
    permkey_3         [table_a_key_3, table_b_key_3, table_c_key_3]
    ================= =============================================
    
    Example::
        
        class Convert(remus.RemapTarget):
            __inputs__ = [ 'gmap', 'amap' ]
        
            def remap(self, srcKey, keys, vals):
                gKey = keys['gmap']
                gValue = vals['gmap']
                
                ....
                
                self.emit(someKey, someValue)
        
        
        
        class ConvertSchedule(remus.Target):
            gTable = self.openTable('genomicMatrix')
            aTable = self.openTable('aliasMap')      
        
            permTable = self.createTable('convReqest')
        
            #...
            #scan gTable and aTable for valid permutations,
            #in the loop, we would have gKey, aKey, and permKey
            #and emit that values
                pTable.emit( permKey, [ gKey, aKey ] )
            #...
                
            #now schedule the remap
            self.addChildTarget("convert_out", Convert(pTable.getPath(), [gTable.getPath(), aTable.getPath()]) )

    
    """

    def __init__(self, keyTable, srcTables, outTableInfo={}):
        self.__keyTable__ = keyTable
        self.__keyTableRef__ = TableRef(keyTable)
        self._srcTables = []
        self._outTable = None
        self._outTableInfo = outTableInfo
        for src in srcTables:
            self._srcTables.append(TableRef(src))
    
    def __run__(self):
        keySrc = self.__manager__._openTable(self.__keyTableRef__.instance, self.__keyTableRef__.table)
        src = {}
        for i, srcName in enumerate(self.__inputs__):
            src[srcName] = self.__manager__._openTable(self._srcTables[i].instance, self._srcTables[i].table)
        for srcKey, keys in keySrc:
            valMap = {}
            i = 0
            for srcName in self.__inputs__:
                valList = []
                for oVal in src[srcName].get(keys[i]):
                    valList.append( (keys[i], oVal) )
                valMap[srcName] = valList
                i += 1
            #print srcKey, self.__inputs__, valMap
            self.__remapCall__(srcKey, self.__inputs__, valMap)
    
    def __remapCall__(self, key, srcNames, inputs, keys={}, vals={}):
        if len(srcNames):
            passKeys = copy(keys)
            passVals = copy(vals)
            for val in inputs[srcNames[0]]:
                passKeys[srcNames[0]] = val[0]
                passVals[srcNames[0]] = val[1]
                self.__remapCall__(key, srcNames[1:], inputs, passKeys, passVals)
        else:
            print key, keys, vals
            self.remap(key, keys, vals)
    
    def copyFrom(self, path, srcTable, key, name):
        """
        Copy file to output table
        
        :param path: Path to store attachment at
        
        :param srcTable: of the table to copy from
        
        :param key: The key the file is attached to
        
        :param name: Name of the attachment
        """
        for i, t in enumerate(self.__inputs__):
            if t == srcTable:
                st = self.__manager__._openTable(self._srcTables[i].instance, self._srcTables[i].table)
                st.copyFrom(path, key, name)
    
    def emit(self, key, value):
        """
        Emit a value to be stored in the output table
        """
        if self._outTable is None:
            self._outTable = self.__manager__._createTable(self.__instance__, self.__tablepath__, self._outTableInfo)
        self._outTable.emit(key, value)
    
    def copyTo(self, path, key, name):
        """
        Copy a file to the output stack
        
        :param path: Path of the file to be copied
        
        :param key: Key the attachment is to be copied to
        
        :param name: 
        
        """
        if self._outTable is None:
            self._outTable = self.__manager__._createTable(self.__instance__, self.__tablepath__, self._outTableInfo)
        self._outTable.copyTo(path, key, name)
    
    def remap(self, srcKey, keys, vals):
        raise Exception("remap method not implemented")

        
class Client(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.transport.open()
        self.client = RemusNet.Client(self.protocol)
    
    def __getattr__(self,i):
        if i in [ 'keySlice', 'addDataJSON', 'getValueJSON', 
        'containsKey', 'initAttachment', 'appendBlock', 'keyValJSONSlice',
        'listAttachments', 'getAttachmentInfo', 'readBlock', 'peerInfo']:
            return getattr(self.client,i)
    
    
    def copyTo(self, path, ar, key, name):
        self.client.initAttachment(ar, key, name)
        handle = open(path)
        while 1:
            line = handle.read(10240)
            if len(line) == 0:
                break 
            self.client.appendBlock(ar, key, name, line)
        handle.close()
    
    def copyFrom(self, path, ar, key, name):
        handle = open(path, "w")
        offset = 0
        while 1:
            line = self.client.readBlock(ar, key, name, offset, 10240)
            if len(line) == 0:
                break 
            offset += len(line)
            handle.write(line)                
        handle.close()
    
    

def getAppletRef(iface, pipeline, instance, applet):
    inst = instance
    ar = RemusNet.AppletRef(pipeline, constants.STATIC_INSTANCE, constants.SUBMIT_APPLET)
    for a in iface.getValueJSON( ar, instance ):
        try:
            inst = json.loads(a)["_instance"]
        except KeyError:
            pass
            
    return RemusNet.AppletRef(pipeline, inst, applet)
    
                
class PeerManager:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None
    
    def connect(self):
        if self.socket is None:
            print self.host, self.port
            self.socket = TSocket.TSocket(self.host, self.port)
            self.transport = TTransport.TBufferedTransport(self.socket)
            self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            self.transport.open()
            self.server = RemusNet.Client(self.protocol)
        
    def close(self):
        self.transport.close()
        
    def getDataServer(self):
        self.connect()
        peers = self.server.peerInfo([])
        for p in peers:
            if p.peerType == RemusNet.PeerType.DB_SERVER:
                return p.peerID
    
    def getAttachServer(self):
        self.connect()
        peers = self.server.peerInfo([])
        for p in peers:
            if p.peerType == RemusNet.PeerType.ATTACH_SERVER:
                return p.peerID

    def getManager(self):
        self.connect()
        peers = self.server.peerInfo([])
        print peers
        for p in peers:
            if p.peerType == RemusNet.PeerType.MANAGER:
                return p.peerID

    
    def getIface(self, peerID):
        peers = self.server.peerInfo([])
        for p in peers:
           if p.peerID == peerID:
              return Client(p.addr.host,p.addr.port)
        return None
    
    def lookupInstance(self,pipeline,instance):
        pid = self.getDataServer()
        iface = self.getIface(pid)        
        ar = RemusNet.AppletRef(pipeline, constants.STATIC_INSTANCE, constants.SUBMIT_APPLET)
        for a in iface.getValueJSON( ar, instance ):
            try:
                return json.loads(a)["_instance"]
            except KeyError:
                pass
        return instance
        
