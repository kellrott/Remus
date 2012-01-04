
import remus.manage
import sys

if __name__ == "__main__":
    config = remus.manage.Config( sys.argv[1], sys.argv[2], sys.argv[3] )
    manager = remus.manage.Manager( config )
    manager.wait()
