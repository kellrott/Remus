
import sys
import remus.manage
import remus.db
import tempfile
import os


if __name__ == "__main__":
    if len(sys.argv) == 5:
        config = remus.manage.Config(sys.argv[1], sys.argv[2])
        worker = remus.manage.Worker(config=config, instance=sys.argv[3], appletPath=sys.argv[4])
        worker.run()
       
    
