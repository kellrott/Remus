#!/usr/bin/env python

import sys
import json
from urllib2 import urlopen
from urllib  import quote
from urlparse import urlparse

try:
    import yaml
except ImportError:
    pass

import imp
from cStringIO import StringIO
import callback
import remusLib
import os


class miniFileCallBack:

    def __init__(self, path, appletName, appletDesc, outdir="mini" ):
        self.appletName = appletName
        self.appletDesc = appletDesc
        self.outdir = outdir
        if not os.path.exists( self.outdir ):
            os.makedirs( self.outdir )
        self.outHandles = { None : open( os.path.join( self.outdir, self.appletName + "@data" ), "w") }
        if '_output' in appletDesc:
            for port in appletDesc['_output']:
                self.outHandles[ port ] = open( os.path.join( self.outdir, self.appletName + "." + port + "@data" ), "w" )
            
            
    def open( self, key, name, mode ):
        if mode == "w":
            handle = open( os.path.join( self.outdir, "%s.%s" % ( key, name ) ), "w" )
            return handle    
    
    def keylist( self, applet ):
        print "stack", self.url + "?info"
        info = json.loads( urlopen( self.url + "?info" ).read() )
        keyURL = self.server + info["_pipeline"] + "/" + applet.replace(":", "/")
        handle = urlopen( keyURL )
        for line in handle:
            yield json.loads( line )

    def get( self, applet, key ):
        print "stack", self.url + "?info"
        info = json.loads( urlopen( self.url + "?info" ).read() )
        keyURL = self.server + info["_pipeline"] + "/" + applet.replace(":", "/") + "/" + key
        handle = urlopen( keyURL )
        for line in handle:
            yield json.loads( line )[ key ]

    def emit(self, key, value, port):
        self.outHandles[ port ].write( "%s\t%s\n" % ( json.dumps(key), json.dumps(value)) )

    def getInfo( self, name ):
        return self.appletDesc.get( name, None ) 
    
    def close( self ):
        for port in self.outHandles:
            self.outHandles.close()


class miniNetCallback(miniFileCallBack):
    def __init__(self, server, pipeline, instance, applet, appletDesc ):
        miniFileCallBack.__init__( self, server + "/" + pipeline + "/" + instance, applet, appletDesc )        
        self.server = server
        self.pipeline = pipeline
        self.instance = instance
        self.applet = applet

    def open( self, key, name, mode ):
        if mode == "w":
            handle = open( os.path.join( self.outdir, "%s.%s" % ( key, name ) ), "w" )
            return handle    
        attachURL = self.server + "/" + self.pipeline + "/" + self.instance + "/" + self.appletDesc["_src"] + "/" + key + "/" + name
        print attachURL
        return urlopen( attachURL )
    
    
    def keylist( self, applet ):
        print "stack", self.url + "?info"
        info = json.loads( urlopen( self.url + "?info" ).read() )
        keyURL = self.server + info["_pipeline"] + "/" + applet.replace(":", "/")
        handle = urlopen( keyURL )
        for line in handle:
            yield json.loads( line )

    def get( self, applet, key ):
        print "stack", self.url + "?info"
        info = json.loads( urlopen( self.url + "?info" ).read() )
        keyURL = self.server + info["_pipeline"] + "/" + applet.replace(":", "/") + "/" + key
        handle = urlopen( keyURL )
        for line in handle:
            yield json.loads( line )[ key ]


class ServerWrapperGen:
    def __init__(self, server, pipeline, instance):
        self.server = server
        self.pipeline = pipeline
        self.instance = instance
    
    def getWrapper( self, applet ):
        return remusLib.StackWrapper( self.server, "DEBUG", self.pipeline, self.instance, applet )

class FileWrapper:
    def __init__(self, path):
        self.path = path
    
    def listKVPairs(self):
        handle = open( self.path )
        for line in handle:
            tmp = line.split("\t")
            yield json.loads(tmp[0]), json.loads( tmp[1] )

class FileWrapperGen:
    def __init__(self, path):
        self.path = path
    
    def getWrapper( self, applet ):
        return FileWrapper( os.path.join( self.path, applet + "@data" ) )

def main( argv ):
    from getopt import getopt
    
    outdir = "mini"
    
    opts, args = getopt( argv, "j:o:" )
    
    jobDesc = {}
    for a,o in opts:
        if a == "-j":
            handle = open( o )
            jobDesc = json.loads( handle.read() )
            handle.close()

        if a == "-o":
            outdir = o
    

    pipePath = args[0]
    applet   = args[1]
    if len(args) > 2:
        input = args[2]
    else:
        input = ""
        
    for arg in args[3:]:
        tmp = arg.split( '=' )
        appletDesc[ tmp[0] ] = tmp[1]

    if pipePath.endswith(".yaml"):
        appletDesc = yaml.load( open( pipePath ).read() )[ applet ]
    else:
        appletDesc = json.loads( open( pipePath ).read() )[ applet ]
    
    if input.startswith( "http://" ) or input.startswith( "https://" ):
        h = urlparse( input )
        server = "%s://%s" % ( h.scheme, h.netloc )
        tmp = h.path.split("/")
        pipeline = tmp[1]
        instance = tmp[2]
        cb = callback.RemusCallback( miniNetCallback(server, pipeline, instance, applet, appletDesc) )
        wrapperFactory = ServerWrapperGen( server, pipeline, instance )
    else:
        cb = callback.RemusCallback( miniFileCallBack(input, applet, appletDesc, outdir) )
        wrapperFactory = FileWrapperGen( input )
    
    print appletDesc

    code = open( appletDesc['_code'] ).read()
    module = imp.new_module( "test_func" )    
    module.__dict__["__name__"] = "test_func"
    module.__dict__["remus"] = cb
    exec code in module.__dict__
    func = cb.getFunction( "test_func" )    
        
    if (appletDesc['_mode'] == "split"):
        func( appletDesc )

    if ( appletDesc['_mode'] == "map" ):
        for dkey, data in remusLib.getDataStack( wrapperFactory.getWrapper( appletDesc["_src"] ) ):
            print "running", dkey
            func( dkey, data )
                
    if ( appletDesc['_mode'] == "reduce" ):
        for dkey, data in remusLib.getDataStack( wrapperFactory.getWrapper( appletDesc["_src"] ), reduce=True ):
            func( dkey, data )
    
    if ( appletDesc['_mode'] == "pipe" ):
        inList = []
        for inFile in appletDesc['_src']:            
            dStack = wrapperFactory.getWrapper( inFile )            
            iHandle = remusLib.getDataStack( dStack )
            inList.append( iHandle )
        func( inList )

        


if __name__=="__main__":
    main( sys.argv[1:] )


