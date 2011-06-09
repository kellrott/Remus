
from remusDB import AbstractStack, AbstractAttach
import remusLib
import os
import json
import urllib
global config
config = None

def quote(inStr):
    return urllib.quote( inStr ).replace('/','%2F')

class FileAttach( AbstractAttach ):
    def __init__(self, server, workerID, pipeline, instance, applet ):
        AbstractAttach.__init__(self, server,workerID, pipeline, instance, applet)
        global config
        if config is None:
            confTxt = remusLib.urlopen( server + "/@db" ).read()
            config = json.loads( confTxt ) 
            if config['attachStore'][ 'type' ] != 'fileSystem' or config['attachStore'][ 'shared' ] != 'true':
                raise Exception()        
        self.baseDir = config['attachStore']['baseDir']
    
    def open(self, key, name, mode="r"):
        path = [ self.baseDir ]
        appletDir = "/%s/%s" % (self.pipeline, self.applet)
        path.append( quote( appletDir ) )
        path.append( quote( self.instance ) )
        path.append( quote(key) )
        path.append( quote(name) )        
        #print "OPEN", path
        pathStr = os.path.join( *path ) 
        if ( mode == 'w' and not os.path.exists( os.path.dirname( pathStr ) )):
            try:   
                os.makedirs( os.path.dirname(pathStr) )
            except OSError:
                pass
        return open( pathStr, mode )

remusLib.setAttachDB( 'file', FileAttach )
