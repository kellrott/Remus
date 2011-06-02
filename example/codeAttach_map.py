
import hashlib
@remus.map
def map( key, val ):
	iHandle = remus.open( key, "test.file" )
	oHandle = remus.open( key + key, "out.file", "w" )
	for line in iHandle:
		oHandle.write("%s\n" % (line.rstrip()) )		
		oHandle.write("%s\n" % (hashlib.sha224(line.rstrip()).hexdigest()) )
	remus.emit( key+key, "out.file" )
	iHandle.close()
	oHandle.close()
