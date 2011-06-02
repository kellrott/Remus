
@remus.split
def split( info ):
	handle = open( "test.file" )
	for line in handle:
		tmp = line.rstrip().split("\t")
		print tmp
		remus.emit( tmp[0], tmp[1] )
	handle.close()
