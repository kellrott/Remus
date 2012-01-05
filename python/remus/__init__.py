
import json

try:
    from remus.net import RemusNet
    from thrift import Thrift
    from thrift.transport import TSocket
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol
    from remus.net import constants
except ImportError:
    pass


from remus.db import FileDB


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
        
        Example::
            
            class MyWorker(remus.Target):
                
                def __init__(self, dataBlock):
                    self.dataBlock = dataBlock
                
                def run(self):
                    c = MyOtherTarget(dataBlock.pass_to_child)
                    self.addChildTarget('child', c )
        
        child_name:
            Unique name of child to be executed
        
        child:
            Target object to be pickled and run as a remote 
        """
        self.__manager__.addChild(self, child_name, child)

    def createTable(self, tableName):
        """
        Create a table to output to

        :param tableName: Name of the table to be opened
        
        :return: :class:`remus.db.table.WriteTable`

        """
        return self.__manager__.createTable(self.__instance__, self.__tablepath__ + ":" + tableName)

    def openTable(self, tableName):
        """
        Open a table input from. By default, tables belong to the parents,
        because they have already finished executing
        
        :param tableName: Name of the table to be opened
        
        :return: :class:`remus.db.table.ReadTable`
        """
        parentTable = ":".join( self.__tablepath__.split(":")[:-1] )
        return self.__manager__.openTable(self.__instance__, parentTable + ":" + tableName)


class SubmitTarget(Target):
    """
    Submission target
    
    This is a target class, but unlike the original target class, which 
    is generated from a stored pickle, the SubmitTarget receives information from
    a JSON style'd data structure from a submission table.
    This allows for it to be instantiated outside of a python environment, ie from the command 
    line or via web request.
    """
    def __init__(self):
        RemusApplet.__init__(self)
    
    def run(self, params):
        """
        The run method is user provided and run in a seperate process,
        possible on a remote node.
        
        params:
            Data structure containing submission data
        """
        raise Exception()


class PipeApplet(RemusApplet):
    def __init__(self):
        self.created_tables = []
    
    def createTable(self, tableName):
        t = FSKeyTable(self.runInfo, tableName, True)
        self.created_tables.append(t)
        return t


class MapApplet(object):
    def __init__(self, inputTable):
        self.input = inputTable
    
    def run(self):
        for key, value in self.input:
            self.map(key, value)



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
        
