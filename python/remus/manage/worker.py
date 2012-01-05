
import sys
import remus.manage
import remus.db
import tempfile
import os


if __name__ == "__main__":
    if len(sys.argv) == 4:
        config = remus.manage.Config(sys.argv[1], sys.argv[2])
        worker = remus.manage.Worker(config=config, appletPath=sys.argv[3])
        worker.run()
       
    
