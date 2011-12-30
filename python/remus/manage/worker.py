
import sys
import remus.manage
import remus.db
import tempfile
import os

class Worker:
    def __init__(self, config, instance, appletPath=None):
        self.config = config
        self.instance = instance
        self.appletPath = appletPath
    
    def run(self):
        db = remus.db.FileDB( self.config.dbpath )
        
        tmpdir = tempfile.mkdtemp(dir="./")
        instRef = remus.db.TableRef(self.instance, "@applet")
        print instRef
        runVal = None
        for val in db.getValue(instRef, self.appletPath):
            runVal = val
        
        for name in db.listAttachments(instRef, self.appletPath):
            opath = os.path.join(tmpdir, name)
            if not os.path.exists(os.path.dirname(opath)):
                os.makedirs(os.path.dirname(opath))
            db.copyFrom(opath, instRef, self.appletPath, name) 

        runClass = runVal['_submitInit'][0]
        sys.path.insert( 0, os.path.abspath(tmpdir) )
        
        manager = remus.manage.Manager(self.config)

        os.chdir(tmpdir)               
        applet = remus.manage.Applet( runClass )
        print applet.getBase()
        
        cls = applet.getClass()
        obj = cls(val)
        obj.__setpath__(self.instance, self.appletPath)
        obj.__setmanager__(manager)
        obj.run()



if __name__ == "__main__":
    if len(sys.argv) == 5:
        worker = Worker(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
        worker.run()
        
        