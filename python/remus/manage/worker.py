
import sys
import remus.manage
import remus.db
import tempfile
import os

if __name__ == "__main__":
    if len(sys.argv) == 5:
        basedir = sys.argv[1]
        workdir = sys.argv[2]
        instance = sys.argv[3]
        appletPath = sys.argv[4]
        
        db = remus.db.FileDB( workdir )
        
        tmpdir = tempfile.mkdtemp(dir="./")
        instRef = remus.db.TableRef(instance, "@applet")
        print instRef
        runVal = None
        for val in db.getValue(instRef, appletPath):
            runVal = val
        
        for name in db.listAttachments(instRef, appletPath):
            opath = os.path.join(tmpdir, name)
            if not os.path.exists(os.path.dirname(opath)):
                os.makedirs(os.path.dirname(opath))
            db.copyFrom(opath, instRef, appletPath, name) 

        runClass = runVal['_submitInit'][0]
        sys.path.insert( 0, os.path.abspath(tmpdir) )
        
        manager = remus.manage.Manager(basedir, workdir)

        os.chdir(tmpdir)               
        applet = remus.manage.Applet( runClass )
        print applet.getBase()
        
        cls = applet.getClass()
        obj = cls(val)
        obj.__setpath__(instance, appletPath)
        obj.__setmanager__(manager)
        obj.run()
