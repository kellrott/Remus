
import remusLib
from remusDB import AbstractStack 
import json

try:
    import pycassa
    
    global pool
    global config
    
    pool = None
    config = None
    
    class PyCassaStack( AbstractStack ):
        def __init__(self, server, workerID, pipeline, instance, applet, jobID=None ):
            AbstractStack.__init__(self, server, workerID, pipeline, instance, applet, jobID )            
            global pool
            global config
    
            if config is None:
                confTxt = remusLib.urlopen( server + "/@db" ).read()
                config = json.loads( confTxt )
            if pool is None:
                pool = pycassa.connect( config['keyspace'], [ '%s:%s' % (config['server'], config['serverPort'] ) ] )
            
            self.col_fam_str = config[ 'columnFamily' ]
            if config[ 'instColumns' ] == 'true':
                self.col_fam_str = config[ 'columnFamily' ] + "_" + instance.replace("-", "")
            self.col_fam = pycassa.ColumnFamily( pool, self.col_fam_str )
            self.row_str = "%s/%s/%s" % ( instance, pipeline, applet )
            
        def get(self, key):
            try:
                vals = self.col_fam.get( self.row_str, super_column=key )
                for key in vals:
                    yield json.loads( vals[ key ] )
            except pycassa.NotFoundException:
                pass
            
        def put(self, key, jobID, emitID, value):
            data = { key : {'%s_%s' % (jobID, emitID) : json.dumps( value ) } }
            self.col_fam.insert( self.row_str, data )
            
        def listKVPairs(self):
            data = self.col_fam.get(  self.row_str )
            for key in data:
                for item in data[ key ]:
                    #print "PYCASSA LIST: ",  self.row_str, key, json.loads( data[key][ item ] )
                    yield key, json.loads( data[key][ item ] )
        def close(self):
            pass
    
    remusLib.setStackDB( 'pycassa', PyCassaStack )
    


except ImportError:
    pass