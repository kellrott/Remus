
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
        def __init__(self, server, workerID, pipeline, instance, applet ):
            AbstractStack.__init__(self, server, workerID, pipeline, instance, applet )            
            global pool
            global config
    
            if config is None:
                confTxt = remusLib.urlopen( server + "/@db" ).read()
                config = json.loads( confTxt )
            if pool is None:
                pool = pycassa.connect( config['dataStore']['keyspace'], [ '%s:%s' % (config['dataStore']['server'], config['dataStore']['serverPort'] ) ] )
            
            self.col_fam_str = config['dataStore'][ 'columnFamily' ]
            if config['dataStore'][ 'instColumns' ] == 'true':
                self.col_fam_str = config['dataStore'][ 'columnFamily' ] + "_" + instance.replace("-", "")
            self.col_fam = pycassa.ColumnFamily( pool, self.col_fam_str )
            self.row_str = "%s/%s/%s" % ( instance, pipeline, applet )
            
            self.cache = {}
            self.cacheMax = 10000
            self.cacheCount = 0
            
        def get(self, key):
            try:
				curStart = ""
				while 1:
					vals = self.col_fam.get( self.row_str, column_start=curStart, super_column=key )
					for key in vals:
						if curStart != key:
							yield json.loads( vals[ key ] )
						curStart = key
					if len(vals) != 100:
						break
            except pycassa.NotFoundException:
                pass
            
        def put(self, key, jobID, emitID, value):
            if not self.cache.has_key( key ):
                self.cache[ key ] = {}            
            self.cache[ key ][ '%s_%s' % (jobID, emitID) ] = json.dumps( value )
            self.cacheCount += 1
            if ( self.cacheCount > self.cacheMax ):
                self.flush()

        def flush(self):
            self.col_fam.batch_insert( { self.row_str : self.cache } )
            self.cache = {}
            self.cacheCount = 0
            
        def listKVPairs(self):
			curStart = ""
			while 1:
				data = self.col_fam.get( self.row_str, column_start=curStart, column_count=100 )
				for elem in data:
					if elem != curStart:
						for key in data[ elem ]:
							yield elem, json.loads( data[ elem ][ key ] )
					curStart = elem
				if len(data) != 100:
					break

        def close(self):
            self.flush()
    
    remusLib.setStackDB( 'pycassa', PyCassaStack )
    


except ImportError:
    pass