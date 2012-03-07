
import json

from remus.net import RemusNet
from remus.net import constants
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

KEY_SLICE_COUNT = 100

class Client(object):
    def __init__(self, host):
        tmp = host.split(':')
        self.host = tmp[0]
        self.port = int(tmp[1])
        self.socket = TSocket.TSocket(self.host, self.port)
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.transport.open()
        self.client = RemusNet.Client(self.protocol)
    
    def getPath(self):
        return "remus://%s:%d" % (self.host, self.port)
        
    def __getattr__(self,i):
        if i in [ 
            'keySlice', 'addDataJSON', 'getValueJSON', 
            'containsKey', 'initAttachment', 'appendBlock', 'listInstances',
            'keyValJSONSlice', 'listAttachments', 'hasAttachment', 
            'getAttachmentInfo', 'readBlock', 'peerInfo', 'hasKey',
            'createInstanceJSON', 'hasTable', 'deleteTable', 
            'listTables', 'deleteInstance']:
            return getattr(self.client,i)
        raise AttributeError()
    
    def createInstance(self, instance, instanceData):
        self.client.createInstanceJSON(instance, json.dumps(instanceData))
    
    def createTable(self, table, tableData):
        self.client.createTableJSON(table, json.dumps(tableData))
    
    def addData(self, table, key, value):
        self.client.addDataJSON(table, key, json.dumps(value))
    
    def getValue(self, table, key):
        for val in self.client.getValueJSON(table, key):
            yield json.loads(val)
    
    def listKeys(self, table):
        firstKey = ""
        while 1:
            tmp = self.client.keySlice(table, firstKey, KEY_SLICE_COUNT)
            nextKey = None
            for k in tmp:
                if k != firstKey:
                    yield k
                    nextKey = k
            if nextKey is None:
                break
            firstKey = nextKey
    
    def listKeyValue(self, table):
        firstKey = ""
        while 1:
            tmp = self.client.keyValJSONSlice(table, firstKey, KEY_SLICE_COUNT)
            nextKey = None
            for keyval in tmp:
                if firstKey != keyval.key:
                    yield keyval.key, json.loads(keyval.valueJson)
                    nextKey = keyval.key
            if nextKey is None:
                break
            firstKey = nextKey
    
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